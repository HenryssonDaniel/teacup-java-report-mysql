# teacup-java-report-mysql
Java **Te**sting Fr**a**mework for **C**omm**u**nication **P**rotocols and Web Services with MySQL

[![Build Status](https://travis-ci.com/HenryssonDaniel/teacup-java-report-mysql.svg?branch=master)](https://travis-ci.com/HenryssonDaniel/teacup-java-report-mysql)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=HenryssonDaniel_teacup-java-report-mysql&metric=coverage)](https://sonarcloud.io/dashboard?id=HenryssonDaniel_teacup-java-report-mysql)
## What ##
This project makes it possible to save logs in a MySQL database rather than just publish on the screen.
## Why ##
Save the logs to a MySQL database so that they are not deleted after each test execution, no matter what test engine you are using.
## How ##
Follow the steps below:
1. Add this repository as a dependency
1. Create a file named teacup.properties in a folder named .teacup in your home folder.
1. Add reporter=io.githb.henryssondaniel.teacup.report.mysql.DefaultReporter to the file
1. Also add
   1. reporter.mysql.password=[password]
   1. reporter.mysql.server.name=[name]
   1. reporter.mysql.user=[user]