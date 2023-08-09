let {callThreadsafeFunction} = require('./index.js');


callThreadsafeFunction( async (...args) => {
console.log(args[1]);
await timeout(2000);
return [4]
});

function timeout(ms) {
    return [new Promise(resolve => setTimeout(() => resolve([3]), ms))];
}

//MyThreadsafeFunction(async (args) => {
//console.log("in testThreadsafeFunction",args);
//return 3;
//})


