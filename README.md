# 🚦 Plataforma Distribuida para Monitoreo de Tráfico en Tiempo Real

Plataforma distribuida para la recolección, procesamiento y análisis en tiempo real de datos de tráfico urbano usando fuentes públicas como Waze.

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/TheRamdomX/Proyecto-Distribuidos)
[![Docker](https://img.shields.io/badge/Docker-Containers-blue)](https://www.docker.com/)
[![Kafka](https://img.shields.io/badge/Kafka-Streaming-orange)](https://kafka.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.9%2B-green)](https://www.python.org/)
[![Selenium](https://img.shields.io/badge/Selenium-Python-brightgreen)](https://www.selenium.dev/documentation/webdriver/)
[![MySQL](https://img.shields.io/badge/MySQL-Database-lightgrey)](https://www.mysql.com/)
[![Redis](https://img.shields.io/badge/Redis-Cache-red)](https://redis.io/)
[![Apache Pig](https://img.shields.io/badge/Apache-Pig-orange)](https://pig.apache.org/)

---

## 📦 Parte 1: Sistema de Recolección y Almacenamiento

### Componentes

1. **Scraper** 🧠: Recopila eventos desde Waze Live Map  
2. **Storage** 📦: Almacena los eventos en MySQL  
3. **Traffic Generator** 🎯: Simula las querys de usuarios con distintas distribuciones
4. **Cache System** 🧊: Recibe las consultas simulando un sistema caché

### Descripción

El sistema extrae, almacena y analiza datos de tráfico en tiempo real desde Waze, utilizando una arquitectura modular con:

- **Kafka** para mensajería distribuida
- **Selenium** para web scraping
- **MySQL** para almacenamiento persistente  
- **Redis** como sistema de caché de alto rendimiento  
- **Docker** para contenerización y despliegue  

### Diagrama de Arquitectura

```mermaid
graph LR
    A[Scraper] -->|Produce| B[(Kafka)]
    B -->|Consume| C[Storage]
    C --> D[(MySQL)]
    D --> E[Traffic Generator]
    E --> B
    B --> F[Cache System]
    F --> G[(Redis)]
    F --> D
```

### Servicios

| 🌐 Servicio          | 🔢 Puerto | 📝 Descripción              |
|---------------------|-----------|------------------------------|
| 🧭 Zookeeper         | 2181      | Coordinación de Kafka        |
| 💬 Kafka             | 9092      | Broker de mensajes           |
| 🗄️ MySQL             | 3306      | Base de datos relacional     |
| ⚡ Redis             | 6379      | Sistema de caché distribuido |
| 🧠 Scraper           | -         | Extracción de datos de Waze  |
| 📦 Storage           | -         | Almacenamiento de eventos    |
| 🎯 Traffic Generator | -         | Generador de consultas       |
| 🧊 Cache System      | -         | Cache con políticas híbridas |

---

## 📦 Parte 2: Sistema de Procesamiento y Análisis

### Componentes Agregados

1.  **Filter System** 🔍: Elimina eventos duplicados y cercanos
2.  **Pig Processor** 🐷: Procesa y analiza los eventos almacenados

### Componentes No Utilizados

1. **Traffic Generator** 🎯: Simula las querys de usuarios con distintas distribuciones
2. **Cache System** 🧊: Recibe las consultas simulando un sistema caché

### Descripción

El sistema procesa los datos recolectados para generar análisis detallados y eliminar redundancias, utilizando:

- **Apache Pig** para procesamiento de datos
- **Fórmulas de distancia** para detección de duplicados
- **Análisis temporal** por hora y tipo de evento
- **Análisis espacial** por comuna y tipo de evento

### Diagrama de Arquitectura

```mermaid
graph LR
    A[(MySQL)] -->|Export| B[Filter System]
    B -->|Filter| C[Pig Processor]
    C -->|Store| D[(Results)]
```

### Servicios

| 🌐 Servicio          | 📝 Descripción              |
|---------------------|------------------------------|
| 🐷 Pig Processor     | Procesamiento de eventos    |
| 🔍 Filter System     | Eliminación de redundancias   |

### Análisis Generados

1. **Análisis por Comuna**
   - Nombre de la comuna
   - Total de eventos por comuna
   - Numero de tipos de eventos por comuna


3. **Análisis por Comuna Detallado**
   - Nombre de la comuna
   - Tipo de evento
   - Total de eventos

5. **Análisis Temporal**
   - Tipo de evento
   - Hora del día
   - Total de eventos

## 🚀 Quick Start

```bash
git clone https://github.com/TheRamdomX/Proyecto-Distribuidos.git
cd Proyecto-Distribuidos
docker-compose up --build
```
