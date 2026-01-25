package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"

	"weatherlink-materialize-to-sql/models"
)

// DeviceRepository handles database operations for devices
type DeviceRepository struct {
	pool *pgxpool.Pool
}

// NewDeviceRepository creates a new DeviceRepository
func NewDeviceRepository(pool *pgxpool.Pool) *DeviceRepository {
	return &DeviceRepository{pool: pool}
}

// LoadAll loads all devices from the database
func (r *DeviceRepository) LoadAll(ctx context.Context) ([]*models.Device, error) {
	log.Println("Loading devices from database...")

	rows, err := r.pool.Query(ctx, `
		SELECT id, lsid, sensor_type, category, manufacturer, product_name, rt_data_structure_type as data_structure_type
		FROM devices
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query devices: %w", err)
	}
	defer rows.Close()

	var devices []*models.Device
	for rows.Next() {
		var d models.Device
		if err := rows.Scan(&d.ID, &d.LSID, &d.SensorType, &d.Category, &d.Manufacturer, &d.ProductName, &d.DataStructureType); err != nil {
			return nil, fmt.Errorf("failed to scan device: %w", err)
		}
		devices = append(devices, &d)
	}

	log.Printf("Loaded %d devices from database", len(devices))
	return devices, nil
}

// Upsert creates or updates a device
func (r *DeviceRepository) Upsert(ctx context.Context, metadata map[string]interface{}) error {
	lsid, ok := metadata["lsid"].(float64)
	if !ok {
		return fmt.Errorf("missing or invalid lsid in metadata")
	}

	// Extract all device fields
	sensorType, _ := metadata["sensor_type"].(float64)
	category, _ := metadata["category"].(string)
	manufacturer, _ := metadata["manufacturer"].(string)
	productName, _ := metadata["product_name"].(string)
	productNumber, _ := metadata["product_number"].(string)
	rainCollectorType, _ := metadata["rain_collector_type"].(float64)
	active, _ := metadata["active"].(bool)
	txID, _ := metadata["tx_id"].(float64)
	portNumber, _ := metadata["port_number"].(float64)
	parentDeviceType, _ := metadata["parent_device_type"].(string)
	parentDeviceName, _ := metadata["parent_device_name"].(string)
	parentDeviceID, _ := metadata["parent_device_id"].(float64)
	parentDeviceIDHex, _ := metadata["parent_device_id_hex"].(string)
	stationID, _ := metadata["station_id"].(float64)
	stationIDUUID, _ := metadata["station_id_uuid"].(string)
	stationName, _ := metadata["station_name"].(string)
	latitude, _ := metadata["latitude"].(float64)
	longitude, _ := metadata["longitude"].(float64)
	elevation, _ := metadata["elevation"].(float64)
	createdDate, _ := metadata["created_date"].(float64)
	modifiedDate, _ := metadata["modified_date"].(float64)

	metadataJSON, _ := json.Marshal(metadata)

	_, err := r.pool.Exec(ctx, `
		INSERT INTO devices (
			lsid, sensor_type, category, manufacturer, product_name,
			product_number, rain_collector_type, active, tx_id, port_number,
			parent_device_type, parent_device_name, parent_device_id, parent_device_id_hex,
			station_id, station_id_uuid, station_name, latitude, longitude, elevation, 
			created_date, modified_date, metadata, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, NOW())
		ON CONFLICT (lsid) DO UPDATE SET
			sensor_type = EXCLUDED.sensor_type,
			category = EXCLUDED.category,
			manufacturer = EXCLUDED.manufacturer,
			product_name = EXCLUDED.product_name,
			product_number = EXCLUDED.product_number,
			rain_collector_type = EXCLUDED.rain_collector_type,
			active = EXCLUDED.active,
			tx_id = EXCLUDED.tx_id,
			port_number = EXCLUDED.port_number,
			parent_device_type = EXCLUDED.parent_device_type,
			parent_device_name = EXCLUDED.parent_device_name,
			parent_device_id = EXCLUDED.parent_device_id,
			parent_device_id_hex = EXCLUDED.parent_device_id_hex,
			station_id = EXCLUDED.station_id,
			station_id_uuid = EXCLUDED.station_id_uuid,
			station_name = EXCLUDED.station_name,
			latitude = EXCLUDED.latitude,
			longitude = EXCLUDED.longitude,
			elevation = EXCLUDED.elevation,
			created_date = EXCLUDED.created_date,
			modified_date = EXCLUDED.modified_date,
			metadata = EXCLUDED.metadata,
			updated_at = NOW()
	`, int(lsid), int(sensorType), category, manufacturer, productName,
		productNumber, intOrNil(rainCollectorType), active, intOrNil(txID), intOrNil(portNumber),
		parentDeviceType, parentDeviceName, intOrNil(parentDeviceID), parentDeviceIDHex,
		intOrNil(stationID), stringOrNil(stationIDUUID), stationName, floatOrNil(latitude), floatOrNil(longitude), floatOrNil(elevation),
		int64OrNil(createdDate), int64OrNil(modifiedDate), metadataJSON)

	return err
}

// UpdateDataStructureType updates the rt_data_structure_type for a device
func (r *DeviceRepository) UpdateDataStructureType(ctx context.Context, lsid, dataStructureType int) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE devices SET rt_data_structure_type = $1, updated_at = NOW() WHERE lsid = $2
	`, dataStructureType, lsid)
	return err
}

// UpdateStationInfo updates station information for all devices at a given station
func (r *DeviceRepository) UpdateStationInfo(ctx context.Context, stationID int, stationName, stationUUID string) error {
	result, err := r.pool.Exec(ctx, `
		UPDATE devices 
		SET station_id = $1, 
		    station_name = $2, 
		    station_id_uuid = $3, 
		    updated_at = NOW() 
		WHERE station_id = $1 OR station_id IS NULL
	`, stationID, stationName, stringOrNil(stationUUID))
	
	if err != nil {
		return err
	}

	rowsAffected := result.RowsAffected()
	log.Printf("Updated %d devices with station info (station_id=%d, name=%s)", rowsAffected, stationID, stationName)
	
	return nil
}

// Helper functions
func intOrNil(val float64) interface{} {
	if val == 0 {
		return nil
	}
	return int(val)
}

func floatOrNil(val float64) interface{} {
	if val == 0 {
		return nil
	}
	return val
}

func int64OrNil(val float64) interface{} {
	if val == 0 {
		return nil
	}
	return int64(val)
}

func stringOrNil(val string) interface{} {
	if val == "" {
		return nil
	}
	return val
}
