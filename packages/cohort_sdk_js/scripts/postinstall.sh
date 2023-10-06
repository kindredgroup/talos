#!/bin/sh
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - Invoked after this module is installed as dependency into node_modules/
# - The content of node_modules/cohort_sdk_js/ will have everything under dist/
#   directory. This script compiles native bindings and then moves the content of built
#   talos package "cohort_sdk_js" from dist/talos/packages/ into the module root,
#   making NPM usable as dependency.
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
echo "Executing scripts/postinstall.sh"

CURRENT_DIR=$(pwd)
echo "D: CURRENT_DIR=$CURRENT_DIR"

IFS='/'
read -a pathTokens <<<"$CURRENT_DIR"

tokensLenth=${#pathTokens[@]}
echo "D: tokensLenth=$tokensLenth"

lenMinusOne=$((tokensLenth-1))
lenMinusTwo=$((tokensLenth-2))
lenMinusThree=$((tokensLenth-3))
echo "D: lenMinusOne=$lenMinusOne, lenMinusTwo=$lenMinusTwo, lenMinusThree=$lenMinusThree"

lastPathToken="${pathTokens[lenMinusTwo]}"/"${pathTokens[lenMinusOne]}"
echo "D: lastPathToken=$lastPathToken"

if [ "$lastPathToken" == "packages/cohort_sdk_js" ]; then
    echo "Post install script of 'cohort_sdk_js' is designed to be executed when module is installed as transitional dependency. Currently, 'npm install' is running at the module itself. Postinstall script is skipped."
    exit 0
fi

lenMinusFour=$((tokensLenth-4))
echo "D: lenMinusOne=$lenMinusOne, lenMinusTwo=$lenMinusTwo, lenMinusThree=$lenMinusThree, lenMinusFour=$lenMinusFour"

lastPathToken="${pathTokens[lenMinusFour]}/${pathTokens[lenMinusThree]}/${pathTokens[lenMinusTwo]}"/"${pathTokens[lenMinusOne]}"
echo "D: lastPathToken=$lastPathToken"

if [ "$lastPathToken" == "cohort_sdk_client/node_modules/@kindredgroup/cohort_sdk_js" ]; then
    echo "Post install script of 'cohort_sdk_js' is designed to be executed when module is installed as transitional dependency. Currently, 'npm install' is running as part of installing 'cohort_sdk_client'. Postinstall script is skipped."
    exit 0
fi

echo "Post install. Current directory is: $CURRENT_DIR"
echo "The content is: "
ls -lah

if [ "$SKIP_NAPI_RS_STEP" == "true" ]; then
    echo "Skipping NAPI RS build step..."
    exit 0
else
    echo "Executing 'npx napi build --platform --release' in 'dist/talos/packages/cohort_sdk_js'"
    cd dist/talos/packages/cohort_sdk_js
    npx napi build --platform --release
fi

returnStatus=$(($?+0))

if [ $returnStatus -eq 0 ]; then
    echo ""
else
    echo ""
    echo "* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *"
    echo "ERROR: The napi command \"$NAPI_CMD\" exited with error status: $returnStatus. Terminating."
    echo "* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *"
    echo ""
    exit $returnStatus
fi

cp ./index.js ./index.d.ts ./package.json "$CURRENT_DIR/"

cp ./cohort_sdk_js* "$CURRENT_DIR/"

cd $CURRENT_DIR

echo "Finishied preparing node module 'cohort_sdk_js'. The content is"
ls -lah
