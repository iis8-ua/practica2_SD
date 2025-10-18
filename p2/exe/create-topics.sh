#!/bin/bash

echo "Creando topics"

#Aqui hay que poner tu ruta
KAFKA_HOME="/home/israelizqdo/Escritorio/3Carrera/SD/Practica/Practica2/Kafka"
BOOTSTRAP_SERVER="localhost:9092"

# Topics de Estados
echo "🔧 Creando topic: cp-estado"
$KAFKA_HOME/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server $BOOTSTRAP_SERVER --topic cp-estado --partitions 1 --replication-factor 1

# Topics de Autorizaciones
echo "🔧 Creando topic: cp-autorizacion" 
$KAFKA_HOME/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server $BOOTSTRAP_SERVER --topic cp-autorizacion --partitions 1 --replication-factor 1

# Topics de Consumo
echo "🔧 Creando topic: actualización-recarga"
$KAFKA_HOME/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server $BOOTSTRAP_SERVER --topic actualización-recarga --partitions 1 --replication-factor 1

# Topics de Alertas
echo "🔧 Creando topic: fallo-cp"
$KAFKA_HOME/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server $BOOTSTRAP_SERVER --topic fallo-cp --partitions 1 --replication-factor 1

# Topics de Recuperaciones
echo "🔧 Creando topic: recuperación-cp"
$KAFKA_HOME/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server $BOOTSTRAP_SERVER --topic recuperación-cp --partitions 1 --replication-factor 1

echo ""
echo "TOPICS CREADOS CORRECTAMENTE"
echo ""
echo "📊 Lista de TUS topics:"
$KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server $BOOTSTRAP_SERVER | grep -E "(cp|actualización|fallo|recuperación)"