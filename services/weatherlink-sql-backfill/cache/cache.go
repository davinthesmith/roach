package cache

import (
	"fmt"
	"sync"

	"weatherlink-sql-backfill/models"
)

// Cache provides thread-safe in-memory caching for devices, tags, and catalog metadata
type Cache struct {
	devices      map[int]*models.Device
	tags         map[string]*models.Tag
	catalog      map[string]map[string]map[string]*models.FieldMetadata // [sensorType][dataStructureType][fieldName]
	mutex        sync.RWMutex
}

// New creates a new Cache instance
func New() *Cache {
	return &Cache{
		devices: make(map[int]*models.Device),
		tags:    make(map[string]*models.Tag),
		catalog: make(map[string]map[string]map[string]*models.FieldMetadata),
	}
}

// GetDevice retrieves a device by LSID
func (c *Cache) GetDevice(lsid int) *models.Device {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.devices[lsid]
}

// SetDevice stores a device in cache
func (c *Cache) SetDevice(lsid int, device *models.Device) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.devices[lsid] = device
}

// GetTag retrieves a tag by device ID and tag name
func (c *Cache) GetTag(deviceID int, tagName string) *models.Tag {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	key := fmt.Sprintf("%d:%s", deviceID, tagName)
	return c.tags[key]
}

// SetTag stores a tag in cache
func (c *Cache) SetTag(deviceID int, tagName string, tag *models.Tag) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	key := fmt.Sprintf("%d:%s", deviceID, tagName)
	c.tags[key] = tag
}

// GetCatalogMetadata retrieves catalog metadata for a specific field
func (c *Cache) GetCatalogMetadata(sensorType, dataStructureType int, fieldName string) *models.FieldMetadata {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	
	stKey := fmt.Sprintf("%d", sensorType)
	dsKey := fmt.Sprintf("%d", dataStructureType)
	
	if c.catalog[stKey] != nil && c.catalog[stKey][dsKey] != nil {
		return c.catalog[stKey][dsKey][fieldName]
	}
	return nil
}

// SetCatalogMetadata stores catalog metadata
func (c *Cache) SetCatalogMetadata(sensorType int, dataStructureType, fieldName string, metadata *models.FieldMetadata) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	
	stKey := fmt.Sprintf("%d", sensorType)
	if c.catalog[stKey] == nil {
		c.catalog[stKey] = make(map[string]map[string]*models.FieldMetadata)
	}
	if c.catalog[stKey][dataStructureType] == nil {
		c.catalog[stKey][dataStructureType] = make(map[string]*models.FieldMetadata)
	}
	c.catalog[stKey][dataStructureType][fieldName] = metadata
}

// Lock acquires write lock for bulk operations
func (c *Cache) Lock() {
	c.mutex.Lock()
}

// Unlock releases write lock
func (c *Cache) Unlock() {
	c.mutex.Unlock()
}
