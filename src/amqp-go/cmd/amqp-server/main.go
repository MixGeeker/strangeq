package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/metrics"
	"github.com/maxpert/amqp-go/server"
)

var version = "0.9.1"

const (
	banner = `
    ___   __  ______  ____        ______      
   /   | /  |/  / __ \/ __ \      / ____/___   
  / /| |/ /|_/ / / / / /_/ /_____/ / __/ __ \  
 / ___ / /  / / /_/ / ____/_____/ /_/ / /_/ /  
/_/  |_/_/  /_/\___\_/          \____/\____/   
                                              
AMQP 0.9.1 Server - High Performance Message Broker
Version: %s
`
)

func main() {
	// Define command-line flags
	var (
		configFile      = flag.String("config", "", "Configuration file path (YAML/JSON)")
		showVersion     = flag.Bool("version", false, "Show version and exit")
		generateConfig  = flag.String("generate-config", "", "Generate default config file and exit (e.g., config.yaml)")
		enableTelemetry = flag.Bool("enable-telemetry", false, "Enable telemetry endpoint (Prometheus + pprof profiling)")
		telemetryPort   = flag.Int("telemetry-port", 9419, "Telemetry HTTP server port")

		// TLS flags
		tlsEnable = flag.Bool("tls", false, "Enable TLS (amqps)")
		tlsCert   = flag.String("tls-cert", "", "Path to TLS certificate file (PEM)")
		tlsKey    = flag.String("tls-key", "", "Path to TLS private key file (PEM)")
		tlsCA     = flag.String("tls-ca", "", "Path to TLS CA file for mutual TLS client verification (PEM)")

		// Recovery override. The broker refuses to start on a data directory
		// whose recovery cannot be completed safely; this is the documented
		// way past that refusal, and the refusal message names it.
		unsafeRecovery = flag.Bool("unsafe-recovery", false,
			"DATA-LOSS SWITCH. Start even when recovery cannot be completed safely, DISCARDING every artifact the broker could not recover. "+
				"Each discarded artifact is logged by path at ERROR level, the amqp_unsafe_recovery_discarded_artifacts gauge is pinned above zero "+
				"for the lifetime of the process, and the broker re-warns every 5 minutes. Confirmed durable messages in the discarded artifacts are "+
				"NOT recovered. Prefer restoring from a backup, or moving the named files aside so you still have them, before using this.")
	)

	flag.Parse()

	// Show version and exit
	if *showVersion {
		fmt.Printf("AMQP-Go Server version %s\n", version)
		return
	}

	// Generate default config and exit
	if *generateConfig != "" {
		cfg := config.DefaultConfig()
		if err := cfg.Save(*generateConfig); err != nil {
			log.Fatalf("Failed to generate config file: %v", err)
		}
		fmt.Printf("Generated default configuration: %s\n", *generateConfig)
		fmt.Println("Edit the file and start server with: amqp-server --config " + *generateConfig)
		return
	}

	// Handle daemon stages first
	if isDaemonChild() {
		// Final daemon process - complete daemonization
		if err := finalizeDaemon(""); err != nil {
			log.Fatalf("Failed to finalize daemon: %v", err)
		}
	}

	// Show banner (only for non-daemon or final daemon process)
	if !isDaemonChild() {
		fmt.Printf(banner, version)
	}

	// Load configuration
	var cfg *config.AMQPConfig
	if *configFile != "" {
		// Load from configuration file
		cfg = &config.AMQPConfig{}
		if err := cfg.Load(*configFile); err != nil {
			log.Fatalf("Failed to load configuration file %s: %v", *configFile, err)
		}
		if !isDaemonChild() {
			fmt.Printf("Loaded configuration from: %s\n", *configFile)
		}
	} else {
		// Use default configuration (can be overridden by AMQP_* environment variables)
		cfg = config.DefaultConfig()
		if !isDaemonChild() {
			fmt.Println("Using default configuration (override with --config or AMQP_* env vars)")
		}
	}

	// Apply TLS command-line flags (override config file values)
	if *tlsEnable {
		cfg.Security.TLSEnabled = true
	}
	if *tlsCert != "" {
		cfg.Security.TLSCertFile = *tlsCert
	}
	if *tlsKey != "" {
		cfg.Security.TLSKeyFile = *tlsKey
	}
	if *tlsCA != "" {
		cfg.Security.TLSCAFile = *tlsCA
	}
	if *unsafeRecovery {
		cfg.Storage.UnsafeRecovery = true
	}

	// Re-validate after CLI overrides
	if err := cfg.Validate(); err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}

	// Handle daemonization if requested (and not already a daemon)
	if cfg.Server.Daemonize && !isDaemonChild() {
		if err := startDaemon(cfg.Server.LogFile); err != nil {
			log.Fatalf("Failed to daemonize: %v", err)
		}
	}

	// Finalize daemon setup if we're the final daemon process
	if isDaemonChild() {
		if err := finalizeDaemon(cfg.Server.LogFile); err != nil {
			log.Fatalf("Failed to finalize daemon: %v", err)
		}
	}

	// Create metrics collector if telemetry is enabled
	var metricsCollector *metrics.Collector
	if *enableTelemetry {
		metricsCollector = metrics.NewCollector("amqp")
		telemetryServer := metrics.NewServer(*telemetryPort, true)

		go func() {
			if !cfg.Server.Daemonize || isDaemonChild() {
				if isDaemonChild() {
					log.Printf("Telemetry server listening on http://localhost:%d", *telemetryPort)
					log.Printf("  Prometheus metrics: http://localhost:%d/metrics", *telemetryPort)
					log.Printf("  Profiling enabled: http://localhost:%d/debug/pprof/", *telemetryPort)
				} else {
					fmt.Printf("Telemetry server listening on http://localhost:%d\n", *telemetryPort)
					fmt.Printf("  Prometheus metrics: http://localhost:%d/metrics\n", *telemetryPort)
					fmt.Printf("  Health check: http://localhost:%d/health\n", *telemetryPort)
					fmt.Printf("  Profiling index: http://localhost:%d/debug/pprof/\n", *telemetryPort)
					fmt.Printf("  CPU profile: curl -o cpu.prof http://localhost:%d/debug/pprof/profile?seconds=30\n", *telemetryPort)
					fmt.Printf("  Mutex profile: curl -o mutex.prof http://localhost:%d/debug/pprof/mutex\n", *telemetryPort)
				}
			}
			if err := telemetryServer.Start(); err != nil {
				log.Printf("Telemetry server failed: %v", err)
			}
		}()
	}

	// Create and start server
	serverBuilder := server.NewServerBuilder().WithConfig(cfg)

	// Add metrics collector if telemetry is enabled
	if metricsCollector != nil {
		serverBuilder = serverBuilder.WithMetrics(metricsCollector)
	}

	amqpServer, err := serverBuilder.Build()
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Write PID file if specified
	if cfg.Server.PidFile != "" {
		if err := writePIDFile(cfg.Server.PidFile); err != nil {
			log.Fatalf("Failed to write PID file: %v", err)
		}
	}

	// Setup signal handling for graceful shutdown
	setupSignalHandling(amqpServer, cfg.Server.PidFile)

	// Start server - show startup info only if not daemonizing
	if !cfg.Server.Daemonize || isDaemonChild() {
		if isDaemonChild() {
			log.Printf("Starting AMQP server daemon on %s", cfg.Network.Address)
			log.Printf("Storage path: %s (persistent)", cfg.Storage.Path)
			if cfg.Security.TLSEnabled {
				if cfg.Security.TLSCAFile != "" {
					log.Println("TLS: Enabled (mutual TLS, client cert required)")
				} else {
					log.Println("TLS: Enabled")
				}
			}
			if cfg.Security.AuthenticationEnabled {
				log.Printf("Authentication: Enabled (%s)", cfg.Security.AuthenticationBackend)
			}
			log.Printf("Transaction support: Enabled")
			log.Printf("Log level: %s", cfg.Server.LogLevel)
			log.Println("AMQP daemon started successfully")
		} else {
			fmt.Printf("Starting AMQP server on %s\n", cfg.Network.Address)
			fmt.Printf("Storage path: %s (persistent)\n", cfg.Storage.Path)
			if cfg.Storage.UnsafeRecovery {
				fmt.Println("*** --unsafe-recovery IS SET: this broker may have discarded unrecoverable data at boot. See the log lines above. ***")
			}
			if cfg.Security.TLSEnabled {
				if cfg.Security.TLSCAFile != "" {
					fmt.Println("TLS: Enabled (mutual TLS, client cert required)")
				} else {
					fmt.Println("TLS: Enabled")
				}
			}
			if cfg.Security.AuthenticationEnabled {
				fmt.Printf("Authentication: Enabled (%s)\n", cfg.Security.AuthenticationBackend)
			}
			fmt.Printf("Transaction support: Enabled\n")
			fmt.Printf("Log level: %s\n", cfg.Server.LogLevel)
			fmt.Println("Server ready - Press Ctrl+C to stop")
		}
	}

	if err := amqpServer.Start(); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func writePIDFile(pidFile string) error {
	pid := os.Getpid()
	return os.WriteFile(pidFile, []byte(fmt.Sprintf("%d\n", pid)), 0644)
}

func setupSignalHandling(server *server.Server, pidFile string) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-c
		fmt.Println("\nShutting down server gracefully...")

		// Clean up PID file
		if pidFile != "" {
			os.Remove(pidFile)
		}

		// Stop server gracefully (cancels metrics goroutine, closes listener)
		server.Stop()

		// Give some time for graceful shutdown
		time.Sleep(2 * time.Second)
		fmt.Println("Server stopped")
		os.Exit(0)
	}()
}
