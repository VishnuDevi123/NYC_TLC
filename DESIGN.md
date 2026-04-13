# System Design - NYC Taxi Trip Analytics Pipeline

## Purpose

This file explains the initial system setup and the role of each component in the project.

The goal is to create a clean local development environment for replaying NYC taxi trip data into Kafka and building streaming analytics on top of it.

---

## Initial Setup Scope

This setup is only for the infrastructure and local development base.

Included:
- Kafka
- Zookeeper
- Python producer container

---

## Current Architecture

```text
NYC TLC Dataset
     |
     v
Python Replay Producer
     |
     v
Kafka Topic(s)