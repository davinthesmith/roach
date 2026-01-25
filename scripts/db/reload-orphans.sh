#!/bin/bash
# Trigger reprocessing of orphaned messages

set -e

CONTAINER_NAME="roach-weatherlink-materializer"

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
    echo "Restarting weatherlink-materializer service to reprocess orphaned messages..."
    docker compose restart weatherlink-materializer
    echo "Service restarted. Monitor logs with: docker compose logs -f weatherlink-materializer"
else
    echo "Cancelled."
fi
