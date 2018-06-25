const Hapi = require('hapi');
const server = new Hapi.Server();
const apiHandlers = require("./graphSearch/apiHandlers.js");
const cacheHandlers = require('./graphSearch/cacheHandlers.js');
// require('events').EventEmitter.prototype._maxListeners = 1000;
const cluster = require('cluster');
const numCPUs = require('os').cpus().length;
console.log('this process isMaster: ' +cluster.isMaster +', isWorker: ' +cluster.isWorker);
if (cluster.isMaster) {
    console.log('this server numCPUs: ' +numCPUs +'核');
}
const log4js = require('log4js');
const req = require('require-yml');
const config = req('./config/source.yml');
const autoFlushCache = config.NodeCache.autoFlushCache;
console.log('程序重启时是否清空缓存: ' +autoFlushCache);

log4js.configure({
    // appenders: {
    //     'out': {
    //         type: 'file',         //文件输出
    //         filename: 'logs/queryDataInfo.log',
    //         maxLogSize: config.logInfo.maxLogSize
    //     }
    // },
    // categories: { default: { appenders: ['out'], level: 'info' } }
    appenders: {
        console: {
            type: 'console'
        },
        log: {
            type: "dateFile",
            filename: "./logs/log4js_log-",
            pattern: "yyyy-MM-dd.log",
            alwaysIncludePattern: true,
            maxLogSize: config.logInfo.maxLogSize
        },
        error: {
            type: "dateFile",
            filename: "./logs/log4js_err-",
            pattern: "yyyy-MM-dd.log",
            alwaysIncludePattern: true,
            maxLogSize: config.logInfo.maxLogSize
        },
        errorFilter: {
            type: "logLevelFilter",
            appender: "error",
            level: "error"
        },
    },
    categories: {
        default: { appenders: ['console', 'log', 'errorFilter'], level: 'debug' }
    },
    pm2: true,
    pm2InstanceVar: 'INSTANCE_ID'
});
const logger = log4js.getLogger('graphTree_search_app');

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

//外部调用接口清空缓存
server.route({
    method: 'GET',
    path: '/flushCache',
    handler: apiHandlers.deleteCacheData
});

server.start((err) => {
    if (err) {
        console.log('server start error: ' +err);
        logger.error('server start error: ' +err);
        process.exit(1);        
        throw err;
    }
    if (autoFlushCache == 'true') {
        cacheHandlers.flushCache();
        console.log('程序初始化, 清除缓存！');
        logger.info('程序初始化, 清除缓存！');
    }
    console.log(`企业关系路径搜索API服务运行在:${server.info.uri}`);
    logger.info(`企业关系路径搜索API服务运行在:${server.info.uri}`);
});
