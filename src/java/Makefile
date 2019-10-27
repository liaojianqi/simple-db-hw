# should be run cmd in current directory

compile:
	javac demo/Demo01.java

run: compile data
	java demo.Demo01

clean:
	rm **/*.class
	rm *.dat

data:
	java -jar ../../dist/simpledb.jar convert some_data_file.txt 3