#!python3

import sys
import math

PARAM_ACTION         = "action"
PARAM_COUNT          = "count"
PARAM_COUNT_ACCOUNTS = "accounts"
PARAM_REPEAT         = "repeat"
PARAM_OUTPUT         = "out"

ACTION_GENERATE_INITIAL      = "gen-initial"
ACTION_GENERATE_TRANSACTIONS = "gen-csv"

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

            line = '{{"name": "{:04d}", "number": "{:04d}", "amount": "{}", "currency": "AUD", "version": 0}}'
            file.write(line.format(i, i, amount))
        file.write("\n]")

#  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
#
#  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
def generateTransactions(params):
    fileName = params[PARAM_OUTPUT]
    fileNameReversed = fileName.replace(".csv", "-reversed.csv")

    try:
        repeat   = int(params[PARAM_REPEAT])
    except:
        repeat = 1

    countAccounts = int(params[PARAM_COUNT_ACCOUNTS])

    total = countAccounts*repeat
    progressEvery = math.ceil(countAccounts*repeat * 20 / 100)
    print(
        "Generating {} transactions into:\n{}\n{}".format(total, fileName, fileNameReversed)
    )

    i = 0
    header = "action,account1,amount,account2"
    with open(fileName, 'w') as file:
        file.write(header)
        for r in range(1, repeat + 1):
            for c in range(1, countAccounts + 1, 2):

                i += 1
                if i % progressEvery == 0:
                    print("...completed {} of {}".format(i, total))

                file.write("\n")
                a1 = c
                a2 = a1 + 1
                if a2 > countAccounts:
                    a2 = a1 - 1
                file.write("TRANSFER,{:04d},1.00,{:04d}".format(a1, a2))

    # Reverse content and store in another file
    with open(fileName, 'r') as file:
        data = file.read().split('\n')

    with open(fileNameReversed, 'w') as file:
        file.write(header)
        for i in range(0, len(data)-1):
            x = len(data) - i - 1
            if x == 0: break

            line = data[x]
            file.write('\n')
            file.write(line)

#  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
#
#  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -
def main(args):
    params = parse(args)
    action = params[PARAM_ACTION]
    if action == ACTION_GENERATE_INITIAL:
        generateInitialData(params)
    elif action == ACTION_GENERATE_TRANSACTIONS:
        generateTransactions(params)
    else:
        print("Unknown action {}".format(action))

if len(sys.argv) == 1:
    print("Usage:")
    print("--action {} --{} '?:int' --{} '?:path' ".format(ACTION_GENERATE_INITIAL, PARAM_COUNT, PARAM_OUTPUT))
    print("\tGenerates JSON file which can be loaded into cohort on startup to be used as its initial state.")
    print("")
    print("--action {} --{} '?:int' --{} '?:int' --{} '?:path' ".format(ACTION_GENERATE_TRANSACTIONS, PARAM_COUNT_ACCOUNTS, PARAM_REPEAT, PARAM_OUTPUT))
    print("\tGenerates CSV file with transactions touching '--{}' acoounts where each account will be used '--{}' times.".format(PARAM_COUNT_ACCOUNTS, PARAM_REPEAT))
    print("\tAdditionally, it will generate another file with 'reverse' suffix where content is the same as '--{}' but in reverse order.".format(PARAM_OUTPUT))
    print("\tIt can be given to the 2nd cohort to avoid conflicts with 1st one.".format(PARAM_OUTPUT))
else:
    main(sys.argv)