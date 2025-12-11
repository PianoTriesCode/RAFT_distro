# RAFT_distro

Implementation of the **Raft Consensus Protocol** in **Go**.

This repository contains a Go-based implementation of *Raft*, a distributed consensus algorithm used to ensure fault-tolerant replication of state machines across a cluster. Raft manages leader election, log replication, and safety guarantees, making it a practical foundation for distributed systems.

---

## 🧠 What is Raft?

Raft is a consensus algorithm designed to be simple and practical. It ensures:

- A **leader** is elected among nodes  
- Log entries are replicated using majority agreement  
- The system remains consistent and fault tolerant  
- Crashed nodes can rejoin without corrupting the log  

Raft is widely used in distributed databases and systems requiring reliability.

---

## 📁 Repository Structure

RAFT_distro/
├── labrpc/ # RPC abstraction layer for network simulation
├── raft/ # Core Raft consensus module
└── README.md # Project documentation


The `labrpc` package simulates unreliable networks for testing.  
The `raft` package holds the full Raft logic.

---

## 🚀 Features

- Leader election  
- Log replication  
- Heartbeats and timeouts  
- Majority-based commit mechanism  
- Crash and restart safety  
- Fully implemented in Go  

---

## 🛠️ Getting Started

### Requirements

- Go 1.18+  
- Git

