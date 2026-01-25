#!/bin/bash
# Database Migration Framework for ROACH
# Simple up/down migration system with tracking

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MIGRATIONS_DIR="$SCRIPT_DIR/migrations"
CONTAINER_NAME="roach-postgres"
DB_USER="roach"
DB_NAME="roach"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Check if PostgreSQL container is running
check_postgres() {
    if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        log_error "PostgreSQL container '${CONTAINER_NAME}' is not running"
        log_info "Start it with: ./scripts/start-infra.sh"
        exit 1
    fi
}

# Execute SQL via docker exec
exec_sql() {
    local sql="$1"
    docker exec -i "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -t -A -c "$sql" 2>&1
}

# Execute SQL file via docker exec
exec_sql_file() {
    local file="$1"
    docker exec -i "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" < "$file" 2>&1
}

# Calculate MD5 checksum of a file
calculate_checksum() {
    local file="$1"
    if [[ "$OSTYPE" == "darwin"* ]]; then
        md5 -q "$file"
    else
        md5sum "$file" | awk '{print $1}'
    fi
}

# Initialize migrations table if it doesn't exist
init_migrations_table() {
    log_info "Checking migrations tracking table..."
    
    local result=$(exec_sql "SELECT to_regclass('public.schema_migrations');")
    
    if [[ "$result" == "" ]] || [[ "$result" == "null" ]]; then
        log_info "Creating schema_migrations table..."
        exec_sql "CREATE TABLE schema_migrations (
            version VARCHAR(100) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            applied_at TIMESTAMP DEFAULT NOW(),
            checksum VARCHAR(32) NOT NULL
        );"
        log_success "Migration tracking table created"
    else
        log_info "Migration tracking table exists"
    fi
}

# Get list of applied migrations
get_applied_migrations() {
    exec_sql "SELECT version FROM schema_migrations ORDER BY version;"
}

# Get list of available migration files
get_available_migrations() {
    if [ ! -d "$MIGRATIONS_DIR" ]; then
        echo ""
        return
    fi
    
    find "$MIGRATIONS_DIR" -name "*.up.sql" -type f | sort | while read -r file; do
        basename "$file" .up.sql
    done
}

# Check if migration is applied
is_migration_applied() {
    local version="$1"
    local applied=$(exec_sql "SELECT COUNT(*) FROM schema_migrations WHERE version='$version';")
    [ "$applied" = "1" ]
}

# Apply a single migration
apply_migration() {
    local version="$1"
    local up_file="$MIGRATIONS_DIR/${version}.up.sql"
    
    if [ ! -f "$up_file" ]; then
        log_error "Migration file not found: $up_file"
        return 1
    fi
    
    # Extract name from filename
    local name=$(echo "$version" | sed 's/^[0-9]*_//' | tr '_' ' ')
    local checksum=$(calculate_checksum "$up_file")
    
    log_info "Applying migration: $version"
    
    # Execute migration in a transaction and capture output
    local output
    output=$({
        echo "BEGIN;"
        cat "$up_file"
        echo "INSERT INTO schema_migrations (version, name, checksum) VALUES ('$version', '$name', '$checksum');"
        echo "COMMIT;"
    } | docker exec -i "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" 2>&1)
    
    local exit_code=$?
    
    # Check if transaction was rolled back or had errors
    if [ $exit_code -ne 0 ] || echo "$output" | grep -qi "ERROR\|ROLLBACK"; then
        log_error "Failed to apply: $version"
        echo "$output" | grep -i "ERROR" >&2
        return 1
    else
        log_success "Applied: $version"
        return 0
    fi
}

# Rollback a single migration
rollback_migration() {
    local version="$1"
    local down_file="$MIGRATIONS_DIR/${version}.down.sql"
    
    if [ ! -f "$down_file" ]; then
        log_error "Rollback file not found: $down_file"
        return 1
    fi
    
    log_info "Rolling back migration: $version"
    
    # Execute rollback in a transaction
    {
        echo "BEGIN;"
        cat "$down_file"
        echo "DELETE FROM schema_migrations WHERE version='$version';"
        echo "COMMIT;"
    } | docker exec -i "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME"
    
    if [ $? -eq 0 ]; then
        log_success "Rolled back: $version"
        return 0
    else
        log_error "Failed to rollback: $version"
        return 1
    fi
}

# Command: migrate up
cmd_up() {
    check_postgres
    init_migrations_table
    
    local available=$(get_available_migrations)
    
    if [ -z "$available" ]; then
        log_warning "No migration files found in $MIGRATIONS_DIR"
        exit 0
    fi
    
    local applied_count=0
    local skipped_count=0
    
    for version in $available; do
        [ -z "$version" ] && continue
        if is_migration_applied "$version"; then
            log_info "Already applied: $version (skipping)"
            ((skipped_count++))
        else
            apply_migration "$version"
            if [ $? -eq 0 ]; then
                ((applied_count++))
            else
                log_error "Migration failed, stopping"
                exit 1
            fi
        fi
    done
    
    echo ""
    log_success "Migrations complete: $applied_count applied, $skipped_count skipped"
}

# Command: migrate down
cmd_down() {
    check_postgres
    init_migrations_table
    
    # Get last applied migration
    local last_migration=$(exec_sql "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1;")
    
    if [ -z "$last_migration" ] || [ "$last_migration" = "" ]; then
        log_warning "No migrations to roll back"
        exit 0
    fi
    
    log_warning "Rolling back migration: $last_migration"
    read -p "Are you sure? (yes/no): " confirm
    
    if [ "$confirm" != "yes" ]; then
        log_info "Rollback cancelled"
        exit 0
    fi
    
    rollback_migration "$last_migration"
}

# Command: migration status
cmd_status() {
    check_postgres
    init_migrations_table
    
    echo ""
    echo "=== Migration Status ==="
    echo ""
    
    local available=$(get_available_migrations)
    
    if [ -z "$available" ]; then
        log_warning "No migration files found"
        exit 0
    fi
    
    for version in $available; do
        [ -z "$version" ] && continue
        if is_migration_applied "$version"; then
            local applied_at=$(exec_sql "SELECT applied_at FROM schema_migrations WHERE version='$version';")
            echo -e "${GREEN}✓${NC} $version (applied: $applied_at)"
        else
            echo -e "${YELLOW}○${NC} $version (pending)"
        fi
    done
    
    echo ""
    
    # Summary
    local total=$(echo "$available" | wc -l | tr -d ' ')
    local applied=$(get_applied_migrations | wc -l | tr -d ' ')
    local pending=$((total - applied))
    
    echo "Total: $total, Applied: $applied, Pending: $pending"
    echo ""
}

# Command: create new migration
cmd_create() {
    local name="$1"
    
    if [ -z "$name" ]; then
        log_error "Migration name required"
        echo "Usage: $0 create <migration_name>"
        exit 1
    fi
    
    # Get next version number
    local last_version=$(get_available_migrations | tail -1 | grep -o "^[0-9]*" || echo "000")
    local next_version=$(printf "%03d" $((10#$last_version + 1)))
    
    # Create filenames
    local base_name="${next_version}_${name}"
    local up_file="$MIGRATIONS_DIR/${base_name}.up.sql"
    local down_file="$MIGRATIONS_DIR/${base_name}.down.sql"
    
    # Create migration files
    mkdir -p "$MIGRATIONS_DIR"
    
    cat > "$up_file" <<EOF
-- Migration: $name
-- Created: $(date +"%Y-%m-%d %H:%M:%S")

-- Add your migration SQL here
-- Example:
-- ALTER TABLE my_table ADD COLUMN new_column VARCHAR(100);

EOF
    
    cat > "$down_file" <<EOF
-- Rollback: $name
-- Created: $(date +"%Y-%m-%d %H:%M:%S")

-- Add your rollback SQL here
-- Example:
-- ALTER TABLE my_table DROP COLUMN new_column;

EOF
    
    log_success "Created migration files:"
    echo "  - $up_file"
    echo "  - $down_file"
    echo ""
    log_info "Edit these files and run './scripts/db/migrate.sh up' to apply"
}

# Main command dispatcher
main() {
    local command="${1:-}"
    
    case "$command" in
        up)
            cmd_up
            ;;
        down)
            cmd_down
            ;;
        status)
            cmd_status
            ;;
        create)
            shift
            cmd_create "$@"
            ;;
        *)
            echo "ROACH Database Migration Tool"
            echo ""
            echo "Usage: $0 <command> [args]"
            echo ""
            echo "Commands:"
            echo "  up      - Apply all pending migrations"
            echo "  down    - Rollback the last applied migration"
            echo "  status  - Show migration status"
            echo "  create  - Create a new migration file"
            echo ""
            echo "Examples:"
            echo "  $0 status"
            echo "  $0 up"
            echo "  $0 down"
            echo "  $0 create add_user_table"
            echo ""
            exit 1
            ;;
    esac
}

main "$@"
