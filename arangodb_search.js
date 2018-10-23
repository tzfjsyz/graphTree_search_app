const Hapi = require('hapi');
const server = new Hapi.Server();
const req = require('require-yml');
const config = req('./config/source.yml');
const apiHandlers = require("./graphSearch/apiHandlers.js");
const cacheHandlers = require('./graphSearch/cacheHandlers.js');
const cluster = require('cluster');
const numCPUs = require('os').cpus().length;
console.log('this process isMaster: ' +cluster.isMaster +', isWorker: ' +cluster.isWorker);
if (cluster.isMaster) {
    console.log('this server numCPUs: ' +numCPUs +'核');
}
const log4js = require('log4js');
const autoFlushCache = config.NodeCache.autoFlushCache;
console.log('程序重启时是否清空缓存: ' +autoFlushCache);

log4js.configure({
    appenders: {
        console: {
            type: 'console'
        },
        log: {
            type: "dateFile",
            filename: "./logs/log4js_log-",
            pattern: "yyyy-MM-dd.log",
            alwaysIncludePattern: true,
            maxLogSize: config.logInfo.maxLogSize,
            backups: 10
        },
        error: {
            type: "dateFile",
            filename: "./logs/log4js_err-",
            pattern: "yyyy-MM-dd.log",
            alwaysIncludePattern: true,
            maxLogSize: config.logInfo.maxLogSize,
            backups: 10
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
const logger = log4js.getLogger('arangodb_search');

server.connection({
    port: 8095,
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

//直接投资关系路径查询
server.route({
    method: 'GET',
    path: '/queryInvestPath',
    handler: apiHandlers.queryInvestPathInfo
});

//直接被投资关系路径查询
server.route({
    method: 'GET',
    path: '/queryInvestedByPath',
    handler: apiHandlers.queryInvestedByPathInfo
});

//共同投资关系路径查询
server.route({
    method: 'GET',
    path: '/queryCommonInvestPath',
    handler: apiHandlers.queryCommonInvestPathInfo
});

//共同被投资关系路径查询
server.route({
    method: 'GET',
    path: '/queryCommonInvestedByPath',
    handler: apiHandlers.queryCommonInvestedByPathInfo
});

//高管投资关系路径查询
server.route({
    method: 'GET',
    path: '/queryExecutiveInvestPath',
    handler: apiHandlers.queryExecutiveInvestPathInfo
});

//担保关系路径查询
server.route({
    method: 'GET',
    path: '/queryGuaranteePath',
    handler: apiHandlers.queryGuaranteePathInfo
});

//被担保关系路径查询
server.route({
    method: 'GET',
    path: '/queryGuaranteedByPath',
    handler: apiHandlers.queryGuaranteedByPathInfo
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

//外部调用接口清空预热的paths
server.route({
    method: 'GET',
    path: '/deleteWarmUpPaths',
    handler: apiHandlers.deleteWarmUpPaths
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
