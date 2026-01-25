#!/bin/bash
# Query database for weather data

set -e

CONTAINER="roach-postgres"
DB_USER="roach"
DB_NAME="roach"

# Check if postgres is running
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
    echo "❌ PostgreSQL is not running"
    exit 1
fi

# Function to run SQL query
query() {
    docker exec -it $CONTAINER psql -U $DB_USER -d $DB_NAME "$@"
}

# Show menu if no arguments
if [ $# -eq 0 ]; then
    echo "🗄️  ROACH Database Query Tool"
    echo "============================"
    echo ""
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  stats           - Show database statistics"
    echo "  devices         - List all devices"
    echo "  tags [device]   - List tags (optionally for specific device LSID)"
    echo "  recent [lsid]   - Show recent records (optionally for specific device)"
    echo "  orphans         - Show orphaned messages"
    echo "  psql            - Open interactive psql session"
    echo ""
    exit 0
fi

case "$1" in
    stats)
        echo "📊 Database Statistics"
        query -c "
        SELECT 
            'Devices' as type, COUNT(*)::text as count FROM devices
        UNION ALL
        SELECT 
            'Tags', COUNT(*)::text FROM tags
        UNION ALL
        SELECT 
            'Numeric Records', COUNT(*)::text FROM records_numeric
        UNION ALL
        SELECT 
            'Text Records', COUNT(*)::text FROM records_text
        UNION ALL
        SELECT 
            'Null Records', COUNT(*)::text FROM records_null
        UNION ALL
        SELECT 
            'Orphaned (pending)', COUNT(*)::text FROM orphaned_messages WHERE NOT reprocessed;
        "
        ;;
    
    devices)
        echo "📱 Devices"
        query -c "
        SELECT 
            lsid,
            category,
            product_name,
            (SELECT COUNT(*) FROM tags WHERE device_id = devices.id) as tags
        FROM devices
        ORDER BY lsid;
        "
        ;;
    
    tags)
        if [ -n "$2" ]; then
            echo "🏷️  Tags for Device LSID=$2"
            query -c "
            SELECT 
                t.tag_name,
                t.data_type,
                (SELECT COUNT(*) FROM records_numeric WHERE tag_id = t.id) +
                (SELECT COUNT(*) FROM records_text WHERE tag_id = t.id) +
                (SELECT COUNT(*) FROM records_null WHERE tag_id = t.id) as record_count
            FROM tags t
            JOIN devices d ON t.device_id = d.id
            WHERE d.lsid = $2
            ORDER BY t.tag_name;
            "
        else
            echo "🏷️  All Tags"
            query -c "
            SELECT 
                d.lsid,
                d.category,
                COUNT(t.id) as tag_count
            FROM devices d
            LEFT JOIN tags t ON d.id = t.device_id
            GROUP BY d.id, d.lsid, d.category
            ORDER BY d.lsid;
            "
        fi
        ;;
    
    recent)
        if [ -n "$2" ]; then
            echo "📈 Recent Records for LSID=$2 (last 20)"
            query -c "
            SELECT 
                TO_TIMESTAMP(r.timestamp) as time,
                t.tag_name,
                r.value,
                r.value_type
            FROM records r
            JOIN tags t ON r.tag_id = t.id
            JOIN devices d ON r.device_id = d.id
            WHERE d.lsid = $2
            ORDER BY r.timestamp DESC
            LIMIT 20;
            "
        else
            echo "📈 Recent Records (last 20)"
            query -c "
            SELECT 
                TO_TIMESTAMP(r.timestamp) as time,
                d.lsid,
                d.category,
                t.tag_name,
                r.value,
                r.value_type
            FROM records r
            JOIN tags t ON r.tag_id = t.id
            JOIN devices d ON r.device_id = d.id
            ORDER BY r.timestamp DESC
            LIMIT 20;
            "
        fi
        ;;
    
    orphans)
        echo "⚠️  Orphaned Messages"
        query -c "
        SELECT 
            topic,
            lsid,
            tag_name,
            reason,
            COUNT(*) as count
        FROM orphaned_messages
        WHERE NOT reprocessed
        GROUP BY topic, lsid, tag_name, reason
        ORDER BY count DESC;
        "
        ;;
    
    psql)
        echo "🔧 Opening interactive psql session..."
        echo "   Database: $DB_NAME, User: $DB_USER"
        echo "   Type \q to exit"
        echo ""
        query
        ;;
    
    *)
        echo "❌ Unknown command: $1"
        echo "Run without arguments to see available commands"
        exit 1
        ;;
esac
