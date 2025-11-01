FROM eclipse-temurin:17-jdk
WORKDIR /app

# Copiar código fuente
COPY . /app

# Compilar el proyecto (si tienes src directamente)
RUN javac -cp .:libs/* EV_Central.java HiloServidor.java ServidorConcurrente.java

# Exponer el puerto de escucha
EXPOSE 8081

# Comando de ejecución
CMD ["java", "-cp", ".:libs/*", "EV_Central", "8081", "kafka:9092", "mariadb:3306"]

