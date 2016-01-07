all:
    -rm mm.jar
    -rm -rf ./build
    mkdir build
    javac -cp `yarn classpath` -d ./build src/*.java
    jar cf mm.jar -C ./build *.class