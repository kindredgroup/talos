#!python3

import sys
import math

PARAM_ACTION         = "action"
PARAM_COUNT          = "count"
PARAM_OUTPUT         = "out"

ACTION_GENERATE_INITIAL      = "gen-initial"

#  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
#
#  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
def parse(args):
    params = {}
    i = 1
    while i < len(args):
        pName = args[i]
        pName = pName.replace("=", "").replace("--", "")

        if (len(args)-1) < (i + 1):
            params[pName] = None
        else:
            params[pName] = args[i+1].strip()

        i += 2

    return params

#  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
#
#  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
def generateInitialData(params):
    fileName = params[PARAM_OUTPUT]
    count = int(params[PARAM_COUNT])

    print("Generating {} accounts into:\n{}".format(count, fileName))

    progressEvery = math.ceil(count * 10 / 100)

    with open(fileName, 'w') as file:
        file.write("[\n")
        amount = "100000.0"
        for i in range(1, count+1):
            if i % progressEvery == 0:
                print("...completed {} of {}".format(i, count))

            if i > 1:
                file.write(",\n")

            line = '{{"name": "{:04d}", "number": "{:04d}", "balance": {}, "version": 0}}'
            file.write(line.format(i, i, amount))
        file.write("\n]")


#  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
#
#  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
def main(args):
    params = parse(args)
    action = params[PARAM_ACTION]
    if action == ACTION_GENERATE_INITIAL:
        generateInitialData(params)
    else:
        print("Unknown action {}".format(action))

if len(sys.argv) == 1:
    print("Usage:")
    print("--action {} --{} '?:int' --{} '?:path' ".format(ACTION_GENERATE_INITIAL, PARAM_COUNT, PARAM_OUTPUT))
    print("\tGenerates JSON file which can be loaded into cohort on startup to be used as its initial state.")
    print("")
else:
    main(sys.argv)