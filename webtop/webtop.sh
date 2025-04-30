#!/bin/bash

# WebTop - Network Traffic Monitor
# This script provides an easy way to start and stop the monitoring services

# Color output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default database connection string
DEFAULT_DB_URI="postgresql://postgres:password@0.0.0.0:5432/website_tracker"

# Function to get database URI
get_db_uri() {
  if [ -n "$DB_URI" ]; then
    echo "$DB_URI"
  else
    echo "$DEFAULT_DB_URI"
  fi
}

# Function to check if using custom URI
is_custom_uri() {
  [ -n "$DB_URI" ]
}

# Function to check database connection
check_db_connection() {
  local uri=$1
  if ! psql "$uri" -c '\q' >/dev/null 2>&1; then
    echo -e "${RED}Error: Could not connect to database at $uri${NC}"
    return 1
  fi
  return 0
}

# Function to manage Docker services
manage_docker_services() {
  local action=$1
  local db_uri=$(get_db_uri)
  
  if is_custom_uri; then
    echo -e "${YELLOW}Using external database - no Docker services to $action${NC}"
    return 0
  fi
  
  echo -e "${YELLOW}Note: This requires Docker privileges${NC}"
  case "$action" in
    "start")
      docker compose up -d
      if [ $? -eq 0 ]; then
        echo -e "${GREEN}Services started successfully!${NC}"
        echo -e "${YELLOW}Waiting for database to be ready...${NC}"
        sleep 5
      fi
      ;;
    "stop")
      docker compose down
      if [ $? -eq 0 ]; then
        echo -e "${GREEN}Services stopped successfully!${NC}"
      fi
      ;;
    "restart")
      docker compose down
      docker compose up -d
      if [ $? -eq 0 ]; then
        echo -e "${GREEN}Services restarted successfully!${NC}"
        echo -e "${YELLOW}Waiting for database to be ready...${NC}"
        sleep 5
      fi
      ;;
    "status")
      docker compose ps
      ;;
  esac
  
  if [ $? -ne 0 ]; then
    echo -e "${RED}Error: Failed to $action services${NC}"
    return 1
  fi
  return 0
}

# Function to run SQL script
run_sql_script() {
  local db_uri=$(get_db_uri)
  local script_path=$1
  local script_name=$2
  
  echo -e "${GREEN}Running $script_name...${NC}"
  psql "$db_uri" -f "$script_path"
  
  if [ $? -eq 0 ]; then
    echo -e "${GREEN}$script_name completed successfully!${NC}"
    return 0
  else
    echo -e "${RED}Error: Failed to run $script_name${NC}"
    return 1
  fi
}

# Function to display usage
show_usage() {
  echo -e "${YELLOW}WebTop${NC} - Network Traffic Monitor"
  echo ""
  echo "Usage: $0 [command] [options]"
  echo ""
  echo "Commands:"
  echo "  start       - Start the monitoring services"
  echo "  stop        - Stop the monitoring services"
  echo "  restart     - Restart the monitoring services"
  echo "  monitor     - Show monitoring dashboard (combines status and patterns)"
  echo "  setup       - Set up traffic patterns for testing"
  echo "  setup-db    - Run the main database setup script"
  echo "  stop        - Stop traffic pattern generation"
  echo "  reset       - Reset the database (clear all data)"
  echo "  stats       - Run advanced statistical analysis"
  echo "  help        - Show this help message"
  echo ""
  echo "Options:"
  echo "  --uri URI   - Use custom database URI (e.g., postgresql://user:pass@host:port/db)"
  echo ""
  echo "Examples:"
  echo "  $0 start --uri postgresql://user:pass@0.0.0.0:5432/mydb"
  echo "  $0 monitor --uri \$demos_uri"
  echo ""
  echo "Proper order of operations:"
  echo "  $0 start"
  echo "  $0 setup-db"
  echo "  $0 setup"
  echo "  $0 monitor"
  echo "  $0 stop"
  echo "Later, when you're done:"
  echo "  $0 reset"
  echo ""
}

# Function to start services
start_services() {
  local script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  local db_uri=$(get_db_uri)
  
  echo -e "${GREEN}Starting WebTop services...${NC}"
  
  # Start Docker services if needed
  if ! manage_docker_services "start"; then
    exit 1
  fi
  
  # Check database connection
  if ! check_db_connection "$db_uri"; then
    echo -e "${RED}Error: Database not ready. Please check connection.${NC}"
    exit 1
  fi
  
  # Run the main setup script
  cd "$script_dir/sql/setup" && PGOPTIONS='--client-min-messages=warning' psql "$db_uri" -X -q -v "ON_ERROR_STOP=1" -f main.sql
  
  if [ $? -eq 0 ]; then
    echo -e "${GREEN}Main setup script completed successfully!${NC}"
    echo -e "Use ${YELLOW}$0 monitor${NC} to see real-time statistics"
  else
    echo -e "${RED}Error: Failed to run main setup script${NC}"
    exit 1
  fi
}

# Function to stop services
stop_services() {
  manage_docker_services "stop"
}

# Function to show unified monitoring dashboard
show_monitoring_dashboard() {
  local db_uri=$(get_db_uri)
  
  # Check database connection
  if ! check_db_connection "$db_uri"; then
    exit 1
  fi
  
  # Check if the traffic_patterns_monitor view exists
  view_exists=$(psql "$db_uri" -t -c "SELECT EXISTS (SELECT FROM pg_views WHERE viewname = 'traffic_patterns_monitor');" | tr -d ' ')
  
  # Clear the screen
  clear
  
  echo -e "${BLUE}=========================================${NC}"
  echo -e "${GREEN}           WebTop Dashboard             ${NC}"
  echo -e "${BLUE}=========================================${NC}"
  echo ""
  
  # Show service status
  if ! is_custom_uri; then
    echo -e "${YELLOW}Service Status:${NC}"
    manage_docker_services "status"
    echo ""
  else
    echo -e "${YELLOW}Database Status:${NC}"
    echo -e "Connected to: $db_uri"
    echo ""
  fi
  
  # Show traffic patterns if available
  if [ "$view_exists" = "t" ]; then
    echo -e "${YELLOW}Traffic Patterns (Last Hour):${NC}"
    psql "$db_uri" -c \
      "SELECT domain, total_requests, avg_requests_per_minute, current_traffic_level, trend 
       FROM traffic_patterns_monitor 
       ORDER BY total_requests DESC;"
    
    echo ""
    
    # Show recent activity
    echo -e "${YELLOW}Recent Activity:${NC}"
    psql "$db_uri" -c \
      "SELECT domain, time, COUNT(*) OVER (PARTITION BY domain) as hits 
       FROM logs 
       WHERE domain LIKE '%-traffic.com' 
       AND time > NOW() - INTERVAL '5 minutes' 
       ORDER BY time DESC LIMIT 10;"
    
    echo ""
    
    # Show job status
    echo -e "${YELLOW}Traffic Generation Jobs:${NC}"
    psql "$db_uri" -c \
      "SELECT j.job_id, j.proc_name, j.schedule_interval, j.next_start
       FROM timescaledb_information.jobs j
       WHERE j.proc_name IN ('generate_log_data_job', 'populate_website_candidates_job', 'elect_top_websites')
       ORDER BY j.job_id;"
    
    echo ""
    
    # Show top websites report
    echo -e "${YELLOW}Top Websites Report:${NC}"
    psql "$db_uri" -c \
      "SELECT domain, total_hits, last_election_hits, times_elected, rank
       FROM top_websites_report
       ORDER BY rank
       LIMIT 5;"
  else
    # Show basic stats if traffic patterns not set up
    echo -e "${YELLOW}Top Domains (Last Hour):${NC}"
    psql "$db_uri" -c \
      "SELECT domain, COUNT(*) AS hits FROM logs 
       WHERE time > NOW() - INTERVAL '1 hour' 
       GROUP BY domain ORDER BY hits DESC LIMIT 10;"
    
    echo ""
    
    echo -e "${YELLOW}Recent Activity:${NC}"
    psql "$db_uri" -c \
      "SELECT domain, time FROM logs 
       ORDER BY time DESC LIMIT 5;"
    
    echo ""
    
    echo -e "${YELLOW}Statistics:${NC}"
    psql "$db_uri" -c \
      "SELECT 'Total Domains' AS stat, COUNT(DISTINCT domain)::text AS value FROM logs
       UNION ALL
       SELECT 'Total Captures' AS stat, COUNT(*)::text AS value FROM logs
       UNION ALL
       SELECT 'Earliest Capture' AS stat, to_char(MIN(now() - time), 'DD HH24:MI:SS') AS value FROM logs
       UNION ALL
       SELECT 'Latest Capture' AS stat, to_char(MAX(now() - time), 'DD HH24:MI:SS') AS value FROM logs;"
  fi
  
  echo ""
  echo -e "${BLUE}=========================================${NC}"
  echo -e "${YELLOW}Press Ctrl+C to exit dashboard${NC}"
  echo -e "${BLUE}=========================================${NC}"
  
  # Keep updating
  sleep 5
  clear
  show_monitoring_dashboard
}

# Function to reset database
reset_database() {
  local db_uri=$(get_db_uri)
  
  # Check database connection
  if ! check_db_connection "$db_uri"; then
    exit 1
  fi
  
  echo -e "${YELLOW}Warning: This will delete all captured data!${NC}"
  read -p "Are you sure you want to continue? (y/n) " -n 1 -r
  echo ""
  
  if [[ $REPLY =~ ^[Yy]$ ]]; then
    run_sql_script "sql/maintenance/reset.sql" "Database reset"
  else
    echo -e "${BLUE}Reset cancelled.${NC}"
  fi
}

# Function to set up traffic patterns
setup_traffic_patterns() {
  local db_uri=$(get_db_uri)
  
  # Check database connection
  if ! check_db_connection "$db_uri"; then
    exit 1
  fi
  
  run_sql_script "sql/setup/setup_traffic_patterns.sql" "Traffic patterns setup"
}

# Function to stop traffic patterns
stop_traffic_patterns() {
  local db_uri=$(get_db_uri)
  
  # Check database connection
  if ! check_db_connection "$db_uri"; then
    exit 1
  fi
  
  run_sql_script "sql/setup/stop_traffic_patterns.sql" "Traffic pattern generation stop"
}

# Function to run main database setup
run_main_setup() {
  local db_uri=$(get_db_uri)
  
  # Check database connection
  if ! check_db_connection "$db_uri"; then
    exit 1
  fi
  
  run_sql_script "sql/setup/main.sql" "Main database setup"
}

# Function to run statistical analysis
run_statistical_analysis() {
  local db_uri=$(get_db_uri)
  
  # Check database connection
  if ! check_db_connection "$db_uri"; then
    exit 1
  fi
  
  run_sql_script "sql/analysis/statistical_analysis.sql" "Statistical analysis"
}

# Function to restart services
restart_services() {
  local script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  local db_uri=$(get_db_uri)
  
  echo -e "${GREEN}Restarting WebTop services...${NC}"
  
  # Restart Docker services if needed
  if ! manage_docker_services "restart"; then
    exit 1
  fi
  
  # Check database connection
  if ! check_db_connection "$db_uri"; then
    echo -e "${RED}Error: Database not ready. Please check connection.${NC}"
    exit 1
  fi
  
  # Run the main setup script
  cd "$script_dir/sql/setup" && PGOPTIONS='--client-min-messages=warning' psql "$db_uri" -X -q -v "ON_ERROR_STOP=1" -f main.sql
  
  if [ $? -eq 0 ]; then
    echo -e "${GREEN}Main setup script completed successfully!${NC}"
    echo -e "Use ${YELLOW}$0 monitor${NC} to see real-time statistics"
  else
    echo -e "${RED}Error: Failed to run main setup script${NC}"
    exit 1
  fi
}

# Parse command line arguments
DB_URI=""
COMMAND=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --uri)
      DB_URI="$2"
      shift 2
      ;;
    start|stop|restart|monitor|setup|setup-db|reset|stats|help)
      COMMAND="$1"
      shift
      ;;
    *)
      echo -e "${RED}Error: Unknown option '$1'${NC}"
      show_usage
      exit 1
      ;;
  esac
done

# Main script execution
if [ -z "$COMMAND" ]; then
  show_usage
  exit 0
fi

# Process commands
case "$COMMAND" in
  start)
    start_services
    ;;
  stop)
    stop_services
    ;;
  restart)
    restart_services
    ;;
  monitor)
    show_monitoring_dashboard
    ;;
  setup)
    setup_traffic_patterns
    ;;
  setup-db)
    run_main_setup
    ;;
  reset)
    reset_database
    ;;
  stats)
    run_statistical_analysis
    ;;
  help)
    show_usage
    ;;
  *)
    echo -e "${RED}Error: Unknown command '$COMMAND'${NC}"
    show_usage
    exit 1
    ;;
esac 
