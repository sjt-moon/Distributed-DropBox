Project 2 starter code
Copyright (C) George Porter, 2017, 2018.

## Overview

This is the starter code for the Java implementation of SurfStore.

## To build the protocol buffer IDL into auto-generated stubs:

$ mvn protobuf:compile protobuf:compile-custom

## To build the code:

$ mvn package

## To run the services:

$ target/surfstore/bin/runBlockServer
$ target/surfstore/bin/runMetadataStore

## To run the client

$ target/surfstore/bin/runClient

## To delete all programs and object files

$ mvn clean

## To upload a file
![upload work flow](https://github.com/sjt-moon/Distributed-DropBox/blob/master/uploadExample.png)

## To download a file
![download work flow](https://github.com/sjt-moon/Distributed-DropBox/blob/master/downloadExample.png)
