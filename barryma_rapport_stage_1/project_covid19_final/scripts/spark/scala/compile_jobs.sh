#!/bin/bash

# Iterate over each Scala script in the current directory
for script in *.scala
do
  # Get the base name of the script
  base=$(basename "$script" .scala)
  
  echo "Compiling $script into $base.jar..."
  
  # Compile the Scala script into a JAR file
  scalac -d "$base.jar" "$script"
  
  echo "$script compiled successfully into $base.jar."
done

echo "All scripts compiled successfully into separate JARs."
