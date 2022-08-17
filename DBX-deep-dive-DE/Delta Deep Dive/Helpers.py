# Databricks notebook source
def file_count (dir_path):
  files = dbutils.fs.ls(deltaPath)
  myCount = -1 # Start with -1 to prevent the "_delta_log" directory from being counted.

  for fileInfo in files:
    myCount = myCount +1

  print(f"There are {myCount} [delta] files.")
