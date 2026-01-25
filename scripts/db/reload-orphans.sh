#!/bin/bash
# Trigger reprocessing of orphaned messages

set -e

CONTAINER_NAME="roach-weather-sql"

echo "Checking orphaned messages..."
docker exec roach-postgres psql -U roach -d roach -c "
SELECT reason, COUNT(*) as count 
FROM orphaned_messages 
WHERE NOT reprocessed 
GROUP BY reason;
"

echo ""
read -p "Do you want to reprocess orphaned messages? (y/n) " -n 1 -r
echo

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Restarting weather-sql service to reprocess orphaned messages..."
    docker compose restart weather-sql
    echo "Service restarted. Monitor logs with: docker compose logs -f weather-sql"
else
    echo "Cancelled."
fi
