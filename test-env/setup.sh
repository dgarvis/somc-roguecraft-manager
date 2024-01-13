cd ..; mvn clean; mvn package; cd test-env; cp ../target/somc-roguecraft-manager-1.0-SNAPSHOT.jar plugins; docker-compose restart roguecraft;
