# VOTECORE — Academic Experiment & System Design Mapping

This document maps the 8 Software System Design (26CMPE512) laboratory requirements to their implementation modules inside the VOTECORE Campus Elections Engine.

| Exp # | Academic Experiment Title | SSD Concept | Implementation Module | Product Workflow | Status |
| :---: | :--- | :--- | :--- | :--- | :---: |
| **1** | REST API Design & CRUD | Resource Layer Decoupling | `modules/exp1_crud/products.js` | Product/Candidate CRUD | **PASS** |
| **2** | Load Balancing | Traffic Distribution | `modules/exp2_loadbalancer/lb_engine.js` | Multi-instance Routing | **PASS** |
| **3** | Redis Cache-Aside | In-Memory Read Acceleration | `modules/exp3_cacheaside/cache_aside.js` | Election Results Lookup | **PASS** |
| **4** | Catalog Benchmark | Relational vs Document Queries | `modules/exp4_catalog/catalog_bench.js` | Manifesto Indexing | **PASS** |
| **5** | RabbitMQ & Rate Limits | Decoupled Queues & Rate Limits | `modules/exp5_rabbitmq_ratelimit/exp5_engine.js` | Login & Ballot Queuing | **PASS** |
| **6** | Locking & Microservices | Atomic Mutex & Service Splitting | `server.js`, `worker.js` | Fast-Path Ballot Locks | **PASS** |
| **7** | Gateway & Observability | Auth, Headers & Logging | `server.js` | Gateway Observability | **PASS** |
| **8** | Software Design Patterns | Adapter, Observer, Strategy | `modules/exp8_patterns/patterns.js` | Notification Delivery | **PASS** |
