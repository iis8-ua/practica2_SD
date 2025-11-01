FROM eclipse-temurin:17-jdk
WORKDIR /app

# Copiamos todo el código del paquete cp
COPY . /app

# Compilamos las clases
RUN javac -cp .:libs/* EV_CP_E.java EV_CP_M.java ChargingPoint.java CPState.java

# Exponemos los puertos (Monitor y Engine)
EXPOSE 8080
EXPOSE 8082

# Variable de entorno que define qué módulo lanzar
ARG MODULE=EV_CP_E
ENV MODULE=${MODULE}

CMD ["sh", "-c", "java -cp .:libs/* ${MODULE} kafka:9092"]

