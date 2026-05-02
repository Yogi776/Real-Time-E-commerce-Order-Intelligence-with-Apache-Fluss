-- =============================================================================
-- 01_create_catalog.sql
-- Creates the Fluss catalog in Flink SQL and sets up the ecommerce database.
-- Run this first before any other SQL files.
-- =============================================================================

-- Create a Fluss catalog that connects Flink to the Fluss cluster.
-- The bootstrap.servers points to the Fluss CoordinatorServer.
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);

-- Switch to the Fluss catalog
USE CATALOG fluss_catalog;

-- Create a dedicated database for the e-commerce analytics platform
CREATE DATABASE IF NOT EXISTS ecommerce;

-- Set the active database
USE ecommerce;
