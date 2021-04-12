import os
import csv 
import sys
from faker import Faker

colors = []
Faker.seed(15)
fake = Faker()
res = {}
rep = 0 
unique = 0
with open(sys.argv[2],"w") as fdw:
    with open(sys.argv[1],"r") as fd:
        lines = fd.readlines()
        print( len(lines ))
        for line in lines:
            line = line.strip()
            if line in res:
                w = line.split(" ")
                line = " ".join( w[1:]  ) + " " + fake.color_name().upper() + " " + w[0]
                rep = rep + 1
                res[line]=1

            else:
                unique = unique + 1
                res[line]=1
            
            fdw.writelines([line + "\n"])

print("Unique: ", unique)
print("Duplicate: ", rep)
