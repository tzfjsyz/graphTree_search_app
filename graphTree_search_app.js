const Hapi = require('hapi');
const server = new Hapi.Server();
const apiHandlers = require("./graphSearch/apiHandlers.js");
// require('events').EventEmitter.prototype._maxListeners = 1000;
const cluster = require('cluster');
const numCPUs = require('os').cpus().length;
console.log('this process isMaster: ' +cluster.isMaster +', isWorker: ' +cluster.isWorker);
if (cluster.isMaster) {
    console.log('this server numCPUs: ' +numCPUs +'核');
}

server.connection({
    port: 8094,
    routes: {
        cors: true
    },
    compression: true
});

//单个企业直接投资关系路径查询
server.route({
    method: 'GET',
    path: '/queryDirectInvestPath',
    handler: apiHandlers.queryDirectInvestPathInfo
});

//单个企业直接被投资关系路径查询
server.route({
    method: 'GET',
    path: '/queryDirectInvestedByPath',
    handler: apiHandlers.queryDirectInvestedByPathInfo
});

//外部接口触发path数据预热
server.route({
    method: 'GET',
    path: '/warmUpPaths',
    handler: apiHandlers.startWarmUpPaths
});

//手动添加需要预加热的from/to/depth/relation
server.route({
    method: 'GET',
    path: '/addWarmUpQueryData',
    handler: apiHandlers.addWarmUpQueryDataInfo
});

//手动删除需要预加热的from/to/depth/relation
server.route({
    method: 'GET',
    path: '/deleteWarmUpQueryData',
    handler: apiHandlers.deleteWarmUpQueryDataInfo
});

//查询所有的预热数据的key对应的field
server.route({
    method: 'GET',
    path: '/listWarmUpConditionsField',
    handler: apiHandlers.listWarmUpConditionsFieldInfo
});

//外部调用接口删除lockResource
server.route({
    method: 'GET',
    path: '/deleteLockResource',
    handler: apiHandlers.deleteLockResource
});

server.start((err) => {
    if (err) {
        console.log('server start error: ' +err);
        process.exit(1);        
        throw err;
    }
    console.info(`企业关系路径搜索API服务运行在:${server.info.uri}`);
});

// process.on('unhandledRejection', (err) => {
//     console.log(err);
//     console.log('NOT exit...');
//     process.exit(1);
// });

// Start the server
// async function start() {

//     try {
//         await server.start();
//     }
//     catch (err) {
//         console.log(err);
//         process.exit(1);
//     }

//     console.log('企业关系路径搜索API服务运行在:', server.info.uri);
// };