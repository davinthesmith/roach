-- ROACH Database Schema
-- Device/Tag/Record hierarchy for weather data materialization
--
-- This is the current production schema (exported from live database)
-- See migrations/ directory for subsequent schema changes

-- PostgreSQL database dump

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: devices; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.devices (
    id integer NOT NULL,
    lsid integer NOT NULL,
    sensor_type integer NOT NULL,
    category character varying(100),
    manufacturer character varying(200),
    product_name character varying(200),
    station_id integer,
    station_id_uuid uuid,
    station_name character varying(200),
    latitude numeric(10,6),
    longitude numeric(10,6),
    elevation numeric(10,3),
    metadata jsonb,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    product_number character varying(100),
    rain_collector_type integer,
    active boolean DEFAULT true,
    tx_id integer,
    port_number integer,
    parent_device_type character varying(100),
    parent_device_name character varying(200),
    parent_device_id bigint,
    parent_device_id_hex character varying(50),
    rt_data_structure_type integer,
    created_date bigint,
    modified_date bigint
);


--
-- Name: COLUMN devices.rt_data_structure_type; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.devices.rt_data_structure_type IS 'Data structure type ID (from real-time current data messages, not available in sensors metadata)';


--
-- Name: COLUMN devices.created_date; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.devices.created_date IS 'Unix timestamp when device was created in WeatherLink (from sensors metadata API)';


--
-- Name: COLUMN devices.modified_date; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.devices.modified_date IS 'Unix timestamp when device was last modified in WeatherLink (from sensors metadata API)';


--
-- Name: devices_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.devices_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: devices_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.devices_id_seq OWNED BY public.devices.id;


--
-- Name: records_null; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.records_null (
    tag_id integer NOT NULL,
    ts bigint NOT NULL
);


--
-- Name: records_numeric; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.records_numeric (
    tag_id integer NOT NULL,
    value numeric,
    ts bigint NOT NULL
);


--
-- Name: records_text; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.records_text (
    tag_id integer NOT NULL,
    value text,
    ts bigint NOT NULL
);


--
-- Name: records; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.records AS
 SELECT records_numeric.tag_id,
    (records_numeric.value)::text AS value,
    'numeric'::text AS value_type,
    records_numeric.ts
   FROM public.records_numeric
UNION ALL
 SELECT records_text.tag_id,
    records_text.value,
    'text'::text AS value_type,
    records_text.ts
   FROM public.records_text
UNION ALL
 SELECT records_null.tag_id,
    NULL::text AS value,
    'null'::text AS value_type,
    records_null.ts
   FROM public.records_null;


--
-- Name: schema_migrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.schema_migrations (
    version character varying(100) NOT NULL,
    name character varying(255) NOT NULL,
    applied_at timestamp without time zone DEFAULT now(),
    checksum character varying(32) NOT NULL
);


--
-- Name: sensor_catalog; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.sensor_catalog (
    id integer NOT NULL,
    sensor_type integer NOT NULL,
    data_structure_type character varying(10) NOT NULL,
    field_name character varying(200) NOT NULL,
    field_type character varying(50),
    units character varying(255),
    description text,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


--
-- Name: TABLE sensor_catalog; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.sensor_catalog IS 'Stores field metadata from WeatherLink sensor catalog API';


--
-- Name: COLUMN sensor_catalog.sensor_type; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.sensor_catalog.sensor_type IS 'Sensor type ID from WeatherLink API';


--
-- Name: COLUMN sensor_catalog.data_structure_type; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.sensor_catalog.data_structure_type IS 'Data structure type ID for this sensor';


--
-- Name: COLUMN sensor_catalog.field_name; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.sensor_catalog.field_name IS 'Name of the data field (e.g., temp, hum)';


--
-- Name: COLUMN sensor_catalog.field_type; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.sensor_catalog.field_type IS 'Data type (float, integer, string)';


--
-- Name: COLUMN sensor_catalog.units; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.sensor_catalog.units IS 'Measurement units (e.g., degrees Fahrenheit) - increased to 255 chars';


--
-- Name: COLUMN sensor_catalog.description; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.sensor_catalog.description IS 'Human-readable description of the field';


--
-- Name: sensor_catalog_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.sensor_catalog_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: sensor_catalog_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.sensor_catalog_id_seq OWNED BY public.sensor_catalog.id;


--
-- Name: tags; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tags (
    id integer NOT NULL,
    device_id integer NOT NULL,
    tag_name character varying(200) NOT NULL,
    data_type character varying(50) NOT NULL,
    unit character varying(255),
    description text,
    metadata jsonb,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


--
-- Name: COLUMN tags.unit; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.tags.unit IS 'Measurement unit for this tag - increased to 255 chars to match catalog';


--
-- Name: tags_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.tags_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: tags_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.tags_id_seq OWNED BY public.tags.id;


--
-- Name: devices id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.devices ALTER COLUMN id SET DEFAULT nextval('public.devices_id_seq'::regclass);


--
-- Name: sensor_catalog id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sensor_catalog ALTER COLUMN id SET DEFAULT nextval('public.sensor_catalog_id_seq'::regclass);


--
-- Name: tags id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tags ALTER COLUMN id SET DEFAULT nextval('public.tags_id_seq'::regclass);


--
-- Name: devices devices_lsid_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.devices
    ADD CONSTRAINT devices_lsid_key UNIQUE (lsid);


--
-- Name: devices devices_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.devices
    ADD CONSTRAINT devices_pkey PRIMARY KEY (id);


--
-- Name: records_null records_null_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.records_null
    ADD CONSTRAINT records_null_pkey PRIMARY KEY (tag_id, ts);


--
-- Name: records_numeric records_numeric_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.records_numeric
    ADD CONSTRAINT records_numeric_pkey PRIMARY KEY (tag_id, ts);


--
-- Name: records_text records_text_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.records_text
    ADD CONSTRAINT records_text_pkey PRIMARY KEY (tag_id, ts);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: sensor_catalog sensor_catalog_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sensor_catalog
    ADD CONSTRAINT sensor_catalog_pkey PRIMARY KEY (id);


--
-- Name: sensor_catalog sensor_catalog_sensor_type_data_structure_type_field_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sensor_catalog
    ADD CONSTRAINT sensor_catalog_sensor_type_data_structure_type_field_name_key UNIQUE (sensor_type, data_structure_type, field_name);


--
-- Name: tags tags_device_id_tag_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tags
    ADD CONSTRAINT tags_device_id_tag_name_key UNIQUE (device_id, tag_name);


--
-- Name: tags tags_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tags
    ADD CONSTRAINT tags_pkey PRIMARY KEY (id);


--
-- Name: idx_devices_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_devices_active ON public.devices USING btree (active);


--
-- Name: idx_devices_category; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_devices_category ON public.devices USING btree (category);


--
-- Name: idx_devices_created_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_devices_created_date ON public.devices USING btree (created_date);


--
-- Name: idx_devices_lsid; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_devices_lsid ON public.devices USING btree (lsid);


--
-- Name: idx_devices_modified_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_devices_modified_date ON public.devices USING btree (modified_date);


--
-- Name: idx_devices_rt_data_structure_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_devices_rt_data_structure_type ON public.devices USING btree (rt_data_structure_type);


--
-- Name: idx_records_null_tag_ts; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_records_null_tag_ts ON public.records_null USING btree (tag_id, ts DESC);


--
-- Name: idx_records_numeric_tag_ts; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_records_numeric_tag_ts ON public.records_numeric USING btree (tag_id, ts DESC);


--
-- Name: idx_records_text_tag_ts; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_records_text_tag_ts ON public.records_text USING btree (tag_id, ts DESC);


--
-- Name: idx_sensor_catalog_field; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_sensor_catalog_field ON public.sensor_catalog USING btree (field_name);


--
-- Name: idx_sensor_catalog_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_sensor_catalog_lookup ON public.sensor_catalog USING btree (sensor_type, data_structure_type);


--
-- Name: idx_tags_device_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tags_device_id ON public.tags USING btree (device_id);


--
-- Name: idx_tags_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tags_name ON public.tags USING btree (tag_name);


--
-- Name: records_null records_null_tag_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.records_null
    ADD CONSTRAINT records_null_tag_id_fkey FOREIGN KEY (tag_id) REFERENCES public.tags(id) ON DELETE CASCADE;


--
-- Name: records_numeric records_numeric_tag_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.records_numeric
    ADD CONSTRAINT records_numeric_tag_id_fkey FOREIGN KEY (tag_id) REFERENCES public.tags(id) ON DELETE CASCADE;


--
-- Name: records_text records_text_tag_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.records_text
    ADD CONSTRAINT records_text_tag_id_fkey FOREIGN KEY (tag_id) REFERENCES public.tags(id) ON DELETE CASCADE;


--
-- Name: tags tags_device_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tags
    ADD CONSTRAINT tags_device_id_fkey FOREIGN KEY (device_id) REFERENCES public.devices(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--
