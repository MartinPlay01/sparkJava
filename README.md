first commit...

##Como instalar el postgres

1) Creamos la imagen

docker build -t userHub/repository:tag .

2) Arrancamos el contenedor

docker run -d --name containerName -p 5432:5432 userHub/repository:tag

docker exec -it containerName bash
