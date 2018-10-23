/*
用于企业关联关系信息查询
wrote by tzf, 2017/12/8
*/
const req = require('require-yml');
const config = req("./config/source.yml");
const pathTreeHandlers = require('./pathTreeHandlers.js');
const moment = require('moment');
const log4js = require('log4js');
const lookupTimeout = config.lookupTimeout;
console.log('lookupTimeout: ' + lookupTimeout + 'ms');
const schedule = require("node-schedule");
const resultHandlers = require('./resultHandlers.js');
const cacheHandlers = require('./cacheHandlers.js');
const arangojs = require("arangojs");
const db = new arangojs.Database({
    url: config.arangodbInfo.url,

});
db.useDatabase(config.arangodbInfo.databaseName);

const warmUpSwitch = config.warmUp_RedisInfo.switch;
const cacheSwitch = config.NodeCache.switch;
console.log(`warmUpSwitch: ${warmUpSwitch}, cacheSwitch: ${cacheSwitch}`);

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

//为每个path追加source/target节点，即增加ITName属性
function setSourceTarget(map, pathDetail) {
    try {
        for (let subPathDetail of pathDetail) {
            for (let subPath of subPathDetail.path) {
                let sourceName = null;
                if (subPath.sourceIsExtra == '1' || subPath.sourceIsExtra == 1 || subPath.sourceIsExtra == 'null') {
                    sourceName = subPath.source;
                }
                else if (subPath.sourceIsExtra == '0' || subPath.sourceIsExtra == 0) {
                    sourceName = map.get(`${subPath.sourceCode}`);
                }
                let targetName = null;
                if (subPath.targetIsExtra == '1' || subPath.targetIsExtra == 1 || subPath.targetIsExtra == 'null') {
                    targetName = subPath.target;
                }
                else if (subPath.targetIsExtra == '0' || subPath.targetIsExtra == 0) {
                    targetName = map.get(`${subPath.targetCode}`);
                }
                subPath.source = sourceName;
                subPath.target = targetName;
            }
        }
        return pathDetail;
    } catch (err) {
        console.error(err);
        logger.error(err);
        return (err);
    }
}

//处理arangodb server 返回的结果
async function handlerQueryResult1(resultPromise, index) {
    let queryNodeResult = {};
    let handlerPromiseStart = Date.now();
    let res = await resultHandlers.handlerPromise1(resultPromise, index);
    let handlerPromiseCost = Date.now() - handlerPromiseStart;
    console.log('handlerPromiseCost: ' + handlerPromiseCost + 'ms');
    logger.info('handlerPromiseCost: ' + handlerPromiseCost + 'ms');
    let beforePathDetail = res.dataDetail.data.pathDetail;                  //没有ITName的pathDetail
    let afterPathDetail = setSourceTarget(res.mapRes, beforePathDetail);
    res.dataDetail.data.pathDetail = afterPathDetail;                       //用带ITName的pathDetail替换原来的
    let newNames = [];
    let nameSet = new Set();
    if (afterPathDetail.length > 1) {
        for (let subPathDetail of afterPathDetail) {
            for (let subPath of subPathDetail.path) {
                nameSet.add(subPath.source);
                nameSet.add(subPath.target);
            }
        }
        for (let subName of nameSet) {
            newNames.push(subName);
        }
    }
    res.dataDetail.names = newNames;
    res.dataDetail.data.pathNum = afterPathDetail.length;
    queryNodeResult.pathDetail = res.dataDetail;
    return queryNodeResult;
}

//处理 server 返回的结果
async function handlerQueryResult2(from, to, resultPromise, j) {
    let queryNodeResult = { nodeResultOne: { pathDetail: {} }, nodeResultTwo: { pathDetail: {} }, nodeResultThree: { pathDetail: {} } };
    let handlerPromiseStart = Date.now();
    let promiseResult = await resultHandlers.handlerPromise2(from, to, resultPromise, j);
    let handlerPromiseCost = Date.now() - handlerPromiseStart;
    console.log('handlerPromiseCost: ' + handlerPromiseCost + 'ms');
    logger.info('handlerPromiseCost: ' + handlerPromiseCost + 'ms');
    let beforePathDetailOne = promiseResult.pathTypeOne.dataDetail.data.pathDetail;                  //没有ITName的pathDetail
    let beforePathDetailTwo = promiseResult.pathTypeTwo.dataDetail.data.pathDetail;
    let beforePathDetailThree = promiseResult.pathTypeThree.dataDetail.data.pathDetail;
    let setSourceTargetStart = Date.now();
    let afterPathDetailOne = setSourceTarget(promiseResult.pathTypeOne.mapRes, beforePathDetailOne);
    let afterPathDetailTwo = setSourceTarget(promiseResult.pathTypeTwo.mapRes, beforePathDetailTwo);
    // let afterPathDetailThree = setSourceTarget(promiseResult.pathTypeThree.mapRes, beforePathDetailThree);
    let setSourceTargetCost = Date.now() - setSourceTargetStart;
    console.log('setSourceTargetCost: ' + setSourceTargetCost + 'ms');
    logger.info('setSourceTargetCost: ' + setSourceTargetCost + 'ms');

    promiseResult.pathTypeOne.dataDetail.data.pathDetail = afterPathDetailOne;                       //用带ITName的pathDetail替换原来的    
    queryNodeResult.nodeResultOne.pathDetail = promiseResult.pathTypeOne.dataDetail;

    promiseResult.pathTypeTwo.dataDetail.data.pathDetail = afterPathDetailTwo;                       //用带ITName的pathDetail替换原来的    
    queryNodeResult.nodeResultTwo.pathDetail = promiseResult.pathTypeTwo.dataDetail;

    promiseResult.pathTypeThree.dataDetail.data.pathDetail = beforePathDetailThree;
    queryNodeResult.nodeResultThree.pathDetail = promiseResult.pathTypeThree.dataDetail;

    return queryNodeResult;
}

//查询arangodb
async function executeQuery(queryBody) {
    if (queryBody) {
        try {
            let cursor = await db.query(queryBody);
            // let result = await cursor.next();
            let result = cursor._result;
            return result;
        } catch (err) {
            console.error(err);
            logger.error(err);
            return err;
        }
    }
    else {
        return [];
    }

}

//为每个path追加增加ITName属性
function setSourceTarget2(map, pathDetail) {
    try {
        for (let subPathDetail of pathDetail) {
            for (let subPath of subPathDetail.path) {
                let compName = null;
                if (subPath.isExtra == '1' || subPath.isExtra == 1 || subPath.isExtra == 'null') {
                    compName = subPath.compName;
                }
                else if (subPath.isExtra == '0' || subPath.isExtra == 0) {
                    compName = map.get(`${subPath.compCode}`);
                }
                subPath.compName = compName;
            }
        }
        return pathDetail;
    } catch (err) {
        console.error(err);
        logger.error(err);
        return (err);
    }
}

//按 注册资本, 名称因子排序
function sortRegName(a, b) {
    let compName_a = a.path[0].compName;
    let RMBRegFund_a = a.path[0].RMBRegFund;
    let compName_b = b.path[0].compName;
    let RMBRegFund_b = b.path[0].RMBRegFund;
    let flag = 0;
    let flagOne = RMBRegFund_b - RMBRegFund_a;                                                                  //注册资本 降序
    let flagTwo = compName_b - compName_a;
    //1. 注册资本 = 0
    if (flagOne == 0) {
        flag = flagTwo;                                                                                          //名称 降序
    }
    else if (flagOne != 0) {
        flag = flagOne;
    }
    return flag;
}

let searchGraph = {

    //单个企业直接投资关系路径查询
    queryDirectInvestPath: async function (code, relation, DIDepth, lowWeight, highWeight, lowFund, highFund, lowSubAmountRMB, highSubAmountRMB, isExtra, isBranches, surStatus, isPerson) {
        return new Promise(async function (resolve, reject) {

            try {
                let j = 6;                                                                                //0代表DirectInvest
                //先从redis中预热的数据中查询是否存在预热值
                let warmUpKey = [code, DIDepth, lowWeight, highWeight, lowFund, highFund, j, relation].join('-');
                let cacheKey = [j, code, relation, DIDepth, lowWeight, highWeight, lowFund, highFund, lowSubAmountRMB, highSubAmountRMB, isExtra, isBranches, surStatus].join('-');
                let warmUpValue = null;
                if (warmUpSwitch == 'on') {
                    warmUpValue = await cacheHandlers.getWarmUpPathsFromRedis(warmUpKey);
                }
                if (!warmUpValue) {
                    let queryBody = null;
                    let surStatusFilter = null;
                    let isExtraFilter = null;
                    let isBranchesFilter = null;
                    let lowFundFilter = null;
                    let highFundFilter = null;
                    let lowWeightFilter = null;
                    let highWeightFilter = null;
                    let lowSubAmountFilter = null;
                    let highSubAmountFilter = null;
                    let nodeCollectionName = config.arangodbInfo.nodeCollectionName;
                    let edgeCollectionName = config.arangodbInfo.edgeCollectionName[`${relation}`];

                    if (relation == 'invests') {
                        if (surStatus == 1 || surStatus == '1') {
                            surStatusFilter = 'filter v.surStatus == 1';
                        }
                        if (isExtra == 0 || isExtra == '0') {
                            isExtraFilter = 'filter v.isExtra == 0';
                        }
                        if (isBranches == 0 || isBranches == '0') {
                            isBranchesFilter = 'filter v.isBranches == 0';
                        }
                        if (lowFund) {
                            lowFundFilter = `filter v.RMBFund >= ${lowFund}`;
                        }
                        if (highFund) {
                            highFundFilter = `filter v.RMBFund <= ${highFund}`;
                        }
                        if (lowWeight) {
                            lowWeightFilter = `filter e.weight >= ${lowWeight}`;
                        }
                        if (highWeight) {
                            highWeightFilter = `filter e.weight <= ${highWeight}`;
                        }
                        if (lowSubAmountRMB) {
                            lowSubAmountFilter = `filter e.subAmountRMB >= ${lowSubAmountRMB}`;
                        }
                        if (highSubAmountRMB) {
                            highSubAmountFilter = `filter e.subAmountRMB <= ${highSubAmountRMB}`;
                        }
                        if (isPerson == 0) {
                            queryBody =
                                `for v, e, p in 1..${DIDepth} outbound '${nodeCollectionName}/${code}' ${edgeCollectionName}
                                    filter v.flag != 1 && e.flag != 1
                                    ${surStatusFilter}
                                    ${isExtraFilter}
                                    ${isBranchesFilter}
                                    ${lowFundFilter}
                                    ${highFundFilter}
                                    ${lowWeightFilter}
                                    ${highWeightFilter}
                                    ${lowSubAmountFilter}
                                    ${highSubAmountFilter}
                                    return distinct p`;
                        }
                        else if (isPerson == 1) {
                            queryBody =
                                `for v, e, p in 1..${DIDepth} outbound '${nodeCollectionName}/${code}' ${edgeCollectionName}
                                    filter v.flag != 1 && e.flag != 1
                                    ${isExtraFilter}
                                    ${lowWeightFilter}
                                    ${highWeightFilter}
                                    ${lowSubAmountFilter}
                                    ${highSubAmountFilter}
                                    return distinct p`;
                        }
                        queryBody = queryBody.replace(/null/g, '');
                    }
                    else if (relation == 'guarantees') {
                        queryBody =
                            `for v, e, p in 1..1 outbound '${nodeCollectionName}/${code}' ${edgeCollectionName} 
                                filter v.flag != 1 && e.flag != 1 
                                return distinct p`;
                    }
                    let resultPromise = null;
                    //获取缓存
                    let previousValue = null;
                    if (cacheSwitch == 'on') {
                        let getCacheStart = Date.now();
                        previousValue = await cacheHandlers.getCache(cacheKey);
                        let getCacheCost = Date.now() - getCacheStart;
                        console.log('directInvestPath_getCacheCost: ' + getCacheCost + 'ms');
                        logger.info('directInvestPath_getCacheCost: ' + getCacheCost + 'ms');
                    }
                    if (!previousValue) {
                        let directInvestPathQueryStart = Date.now();
                        let retryCount = 0;
                        do {
                            try {
                                resultPromise = await executeQuery(queryBody);
                                break;
                            } catch (err) {
                                retryCount++;
                                console.error(err);
                                logger.error(err);
                            }
                        } while (retryCount < 3)
                        if (retryCount == 3) {
                            console.error('executeQuery execute fail after trying 3 times: ' + queryBody);
                            logger.error('executeQuery execute fail after trying 3 times: ' + queryBody);
                        }
                        console.log('query arangodb server: ' + queryBody);
                        logger.info('query arangodb server: ' + queryBody);
                        let directInvestPathQueryCost = Date.now() - directInvestPathQueryStart;
                        logger.info(`${code} DirectInvestPathQueryCost: ` + directInvestPathQueryCost + 'ms');
                        console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + `, ${code} DirectInvestPathQueryCost: ` + directInvestPathQueryCost + 'ms');
                        if (resultPromise.length > 0) {
                            let result = await handlerQueryResult1(resultPromise, j);
                            let nodeResult = result.pathDetail.data.pathDetail;
                            if (nodeResult.length > 0) {
                                let sortTreeResult = null;
                                sortTreeResult = pathTreeHandlers.fromTreePath1(nodeResult, code, relation);
                                if (cacheSwitch == 'on') {
                                    cacheHandlers.setCache(cacheKey, JSON.stringify(sortTreeResult));
                                }
                                if (warmUpSwitch == 'on') {
                                    //根据预热条件的阈值判断是否要加入预热
                                    let queryCostUp = config.warmUp_Condition.queryCost;
                                    let recordsUp = config.warmUp_Condition.queryRecords;
                                    if (directInvestPathQueryCost >= queryCostUp || resultPromise.length >= recordsUp) {
                                        let conditionsKey = config.redisKeyName.warmUpITCodes;
                                        let conditionsField = code;
                                        let conditionsValue = { ITCode: code, depth: [DIDepth], modes: [j], relations: [relation] };                                        //0代表对外投资，1代表股东
                                        cacheHandlers.setWarmUpConditionsToRedis(conditionsKey, conditionsField, JSON.stringify(conditionsValue));
                                        cacheHandlers.setWarmUpPathsToRedis(warmUpKey, JSON.stringify(sortTreeResult));
                                    }
                                }
                                return resolve(sortTreeResult);
                            }
                            else if (nodeResult.length == 0) {
                                let treeResult = { subLeaf: [], subLeafNum: 0 };
                                if (cacheSwitch == 'on') {
                                    cacheHandlers.setCache(cacheKey, JSON.stringify(treeResult));
                                }
                                return resolve(treeResult);
                            }
                        }
                        else if (resultPromise.length == 0) {
                            let treeResult = { subLeaf: [], subLeafNum: 0 };
                            if (cacheSwitch == 'on') {
                                cacheHandlers.setCache(cacheKey, JSON.stringify(treeResult));
                            }
                            return resolve(treeResult);
                        }
                    }
                    else if (previousValue) {
                        return resolve(JSON.parse(previousValue));
                    }
                }
                else if (warmUpValue) {
                    warmUpValue = JSON.parse(warmUpValue);
                    console.log('get the warmUpValue from redis, the key is: ' + warmUpKey);
                    logger.info('get the warmUpValue from redis, the key is: ' + warmUpKey);
                    return resolve(warmUpValue);
                }
            } catch (err) {
                console.error('queryDirectInvestPathError: ' + err);
                logger.error('queryDirectInvestPathError: ' + err);
                return reject(err);
            }
        })
    },

    //单个企业直接被投资关系路径查询
    queryDirectInvestedByPath: async function (code, relation, DIBDepth, lowWeight, highWeight, lowFund, highFund, lowSubAmountRMB, highSubAmountRMB, isExtra) {
        return new Promise(async function (resolve, reject) {

            try {
                let j = 7;                                                                            //1代表DirectInvestedBy
                let warmUpKey = [code, DIBDepth, lowWeight, highWeight, lowSubAmountRMB, highSubAmountRMB, j, relation].join('-');
                let cacheKey = [j, code, relation, DIBDepth, lowWeight, highWeight, lowFund, highFund, lowSubAmountRMB, highSubAmountRMB, isExtra].join('-');
                let warmUpValue = null;
                if (warmUpSwitch == 'on') {
                    //先从redis中预热的数据中查询是否存在预热值
                    warmUpValue = await cacheHandlers.getWarmUpPathsFromRedis(warmUpKey);
                }
                if (!warmUpValue) {
                    let queryBody = null;
                    let isExtraFilter = null;
                    let lowFundFilter = null;
                    let highFundFilter = null;
                    let lowWeightFilter = null;
                    let highWeightFilter = null;
                    let lowSubAmountFilter = null;
                    let highSubAmountFilter = null;
                    let nodeCollectionName = config.arangodbInfo.nodeCollectionName;
                    let edgeCollectionName = config.arangodbInfo.edgeCollectionName[`${relation}`];

                    if (relation == 'invests') {
                        if (isExtra == 0 || isExtra == '0') {
                            isExtraFilter = 'filter v.isExtra == 0';
                        }
                        if (lowFund) {
                            lowFundFilter = `filter v.RMBFund >= ${lowFund}`;
                        }
                        if (highFund) {
                            highFundFilter = `filter v.RMBFund <= ${highFund}`;
                        }
                        if (lowWeight) {
                            lowWeightFilter = `filter e.weight >= ${lowWeight}`;
                        }
                        if (highWeight) {
                            highWeightFilter = `filter e.weight <= ${highWeight}`;
                        }
                        if (lowSubAmountRMB) {
                            lowSubAmountFilter = `filter e.subAmountRMB >= ${lowSubAmountRMB}`;
                        }
                        if (highSubAmountRMB) {
                            highSubAmountFilter = `filter e.subAmountRMB <= ${highSubAmountRMB}`;
                        }
                        queryBody =
                            `for v, e, p in 1..${DIBDepth} inbound '${nodeCollectionName}/${code}' ${edgeCollectionName}
                                filter v.flag != 1 && e.flag != 1
                                ${isExtraFilter}
                                ${lowFundFilter}
                                ${highFundFilter}
                                ${lowWeightFilter}
                                ${highWeightFilter}
                                ${lowSubAmountFilter}
                                ${highSubAmountFilter}
                                return distinct p`;
                        queryBody = queryBody.replace(/null/g, '');
                    }
                    else if (relation == 'guarantees') {
                        queryBody =
                            `for v, e, p in 1..1 inbound '${nodeCollectionName}/${code}' ${edgeCollectionName} 
                                filter v.flag != 1 && e.flag != 1 
                                return distinct p`;
                    }
                    let resultPromise = null;
                    let previousValue = null;
                    if (cacheSwitch == 'on') {
                        let getCacheStart = Date.now();
                        //获取缓存
                        previousValue = await cacheHandlers.getCache(cacheKey);
                        let getCacheCost = Date.now() - getCacheStart;
                        console.log('directInvestedByPath_getCacheCost: ' + getCacheCost + 'ms');
                        logger.info('directInvestedByPath_getCacheCost: ' + getCacheCost + 'ms');
                    }
                    if (!previousValue) {
                        let directInvestedByPathQueryStart = Date.now();
                        let retryCount = 0;
                        do {
                            try {
                                resultPromise = await executeQuery(queryBody);
                                break;
                            } catch (err) {
                                retryCount++;
                                console.error(err);
                                logger.error(err);
                            }
                        } while (retryCount < 3)
                        if (retryCount == 3) {
                            console.error('executeQuery execute fail after trying 3 times: ' + queryBody);
                            logger.error('executeQuery execute fail after trying 3 times: ' + queryBody);
                        }
                        console.log('query arangodb server: ' + queryBody);
                        logger.info('query arangodb server: ' + queryBody);
                        let directInvestedByPathQueryCost = Date.now() - directInvestedByPathQueryStart;
                        logger.info(`${code} directInvestedByPathQueryCost: ` + directInvestedByPathQueryCost + 'ms');
                        console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + `, ${code} directInvestedByPathQueryCost: ` + directInvestedByPathQueryCost + 'ms');
                        if (resultPromise.length > 0) {
                            let result = await handlerQueryResult(resultPromise, j);
                            let nodeResult = result.pathDetail.data.pathDetail;
                            if (nodeResult.length > 0) {
                                let sortTreeResult = pathTreeHandlers.fromTreePath2(nodeResult, code, relation);
                                if (cacheSwitch == 'on') {
                                    cacheHandlers.setCache(cacheKey, JSON.stringify(sortTreeResult));
                                }
                                if (warmUpSwitch == 'on') {
                                    //根据预热条件的阈值判断是否要加入预热
                                    let queryCostUp = config.warmUp_Condition.queryCost;
                                    let recordsUp = config.warmUp_Condition.queryRecords;
                                    if (directInvestedByPathQueryCost >= queryCostUp || resultPromise.length >= recordsUp) {
                                        let conditionsKey = config.redisKeyName.warmUpITCodes;
                                        let conditionsField = code;
                                        let conditionsValue = { ITCode: code, depth: [DIBDepth], modes: [j], relations: [relation] };                                        //0代表对外投资，1代表股东
                                        cacheHandlers.setWarmUpConditionsToRedis(conditionsKey, conditionsField, JSON.stringify(conditionsValue));
                                        cacheHandlers.setWarmUpPathsToRedis(warmUpKey, JSON.stringify(sortTreeResult));
                                    }
                                }
                                return resolve(sortTreeResult);
                            }
                            else if (nodeResult.length == 0) {
                                let treeResult = { subLeaf: [], subLeafNum: 0 };
                                if (cacheSwitch == 'on') {
                                    cacheHandlers.setCache(cacheKey, JSON.stringify(treeResult));
                                }
                                return resolve(treeResult);
                            }
                        }
                        else if (resultPromise.length == 0) {
                            let treeResult = { subLeaf: [], subLeafNum: 0 };
                            if (cacheSwitch == 'on') {
                                cacheHandlers.setCache(cacheKey, JSON.stringify(treeResult));
                            }
                            return resolve(treeResult);
                        }
                    }
                    else if (previousValue) {
                        return resolve(JSON.parse(previousValue));
                    }
                }
                else if (warmUpValue) {
                    warmUpValue = JSON.parse(warmUpValue);
                    console.log('get the warmUpValue from redis, the key is: ' + warmUpKey);
                    logger.info('get the warmUpValue from redis, the key is: ' + warmUpKey);
                    return resolve(warmUpValue);
                }

            } catch (err) {
                console.error('queryDirectInvestedByPathError: ' + err);
                logger.error('queryDirectInvestedByPathError: ' + err);
                return reject(err);
            }
        })
    },

    //直接投资关系的路径查询
    queryInvestPath: async function (from, to, IVDepth, lowWeight, highWeight, pathType) {
        return new Promise(async function (resolve, reject) {

            try {
                let fromIsPerson = 0;
                let toIsPerson = 0;
                if (from.indexOf('P') >= 0) {
                    fromIsPerson = 1;
                }
                if (to.indexOf('P') >= 0) {
                    toIsPerson = 1;
                }
                let j = 0; 
                let warmUpKey = [from, to, IVDepth, j, pathType].join('-');
                let cacheKey = [j, from, to, IVDepth, lowWeight, highWeight, pathType].join('-');
                //记录每种路径查询方式索引号,investPathQuery索引为0
                let warmUpValue = null;
                if (warmUpSwitch == 'on') {
                    //先从redis中预热的数据中查询是否存在预热值
                    warmUpValue = await cacheHandlers.getWarmUpPathsFromRedis(warmUpKey);
                }
                if (!warmUpValue) {
                    if (from == null || to == null) {
                        console.error(`${from} or ${to} is not in the database at all !`);
                        logger.error(`${from} or ${to} is not in the database at all !`);
                        let nodeResults = {
                            nodeResultOne: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${j}`, typeName: 'InvestPath', names: [], codes: [] } },
                            nodeResultTwo: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_10`, typeName: 'guarantees', names: [], codes: [] } },
                            nodeResultThree: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_11`, typeName: 'family', names: [], codes: [] } }
                        };
                        return resolve(nodeResults);
                    }
                    else if (from != null && to != null) {
                        let queryBody = null;
                        let lowWeightFilter = null;
                        let highWeightFilter = null;
                        let edgeCollectionName = null;
                        if (lowWeight) {
                            lowWeightFilter = `filter e.weight >= ${lowWeight}`;
                        }
                        if (highWeight) {
                            highWeightFilter = `filter e.weight <= ${highWeight}`;
                        }
                        let nodeCollectionName = config.arangodbInfo.nodeCollectionName;
                        let invest = config.arangodbInfo.edgeCollectionName.invests;
                        let family = config.arangodbInfo.edgeCollectionName.family;
                        let guanrantee = config.arangodbInfo.edgeCollectionName.guarantees;
                        let queryFirst =
                                        `
                                        let from = '${nodeCollectionName}/${from}'
                                        let to = '${nodeCollectionName}/${to}'
                                        for v, e in 1..2 outbound from ${invest}
                                            filter v.flag != 1 && e.flag != 1
                                            collect with count into lf
                                        for v, e in 1..2 inbound to ${invest}
                                            filter v.flag != 1 && e.flag != 1
                                            collect with count into lt
                                        let s = (lf <= lt ? '${from}' : '${to}')
                                        let t = (lf > lt ? '${from}' : '${to}')
                                        let direction = (lf <= lt ? 'outbound' : 'inbound')
                                        return {s, t, lf, lt, direction}
                                        `;
                        console.log('queryFirst: ', queryFirst);
                        logger.info('queryFirst: ', queryFirst);
                        let queryFirstStart = Date.now();
                        let retryCount = 0;
                        let resultOne = null;
                        do {
                            try {
                                resultOne = await executeQuery(queryFirst);
                                break;
                            } catch (err) {
                                retryCount++;
                                console.error(err);
                                logger.error(err);
                                return reject(err);
                            }
                        } while (retryCount < 3)
                        if (retryCount == 3) {
                            console.error('queryFirst execute fail after trying 3 times: ' + queryBody);
                            logger.error('queryFirst execute fail after trying 3 times: ' + queryBody);
                        }
                        let queryFirstCost = Date.now() - queryFirstStart;
                        let source = null;
                        let target = null;
                        let direction = null;
                        if (resultOne.length > 0 && resultOne[0].hasOwnProperty('direction')) {
                            source = resultOne[0].s;
                            target = resultOne[0].t;
                            sourceLen = resultOne[0].lf;
                            targetLen = resultOne[0].lt;
                            direction = resultOne[0].direction;
                            console.log(`source: ${source}, sourceLen: ${sourceLen}, target: ${target}, targetLen: ${targetLen}, direction is: ${direction}, queryFirstCost: ${queryFirstCost} ms`);
                            logger.info(`source: ${source}, sourceLen: ${sourceLen}, target: ${target}, targetLen: ${targetLen}, direction is: ${direction}, queryFirstCost: ${queryFirstCost} ms`);
                        }
                        else {
                            console.error('queryFirst failure!');
                            logger.error('queryFirst failure!');
                            return reject(err);
                        }
                        if (pathType == 'invests' || !pathType) {
                            edgeCollectionName = invest;

                        }
                        else if (pathType == 'all') {
                            //from/to均为自然人时只找family关系
                            if (fromIsPerson == 1 && toIsPerson == 1) {
                                edgeCollectionName = family;

                            }
                            else if (fromIsPerson == 1 || toIsPerson == 1) {
                                edgeCollectionName = `${invest}, any ${family}, ${guanrantee}`;
                            }

                            else if (fromIsPerson != 1 && toIsPerson != 1) {
                                edgeCollectionName = `${invest}, ${guanrantee}`;
                            }
                        }
                        queryBody =
                                    `
                                    for source in nodes
                                        filter source.flag != 1
                                        filter source._key == '${source}'
                                        limit 1
                                    for v, e, p
                                        in 1..${IVDepth} ${direction} source
                                        ${edgeCollectionName}
                                        filter v.flag != 1 && e.flag != 1
                                        filter p.edges[*].flag all != 1
                                        filter v._key == '${target}'
                                        ${lowWeightFilter}
                                        ${highWeightFilter}
                                        return distinct p
                                    `;
                        queryBody = queryBody.replace(/null/g, '');

                        let now = 0;
                        let previousValue = null;
                        if (cacheSwitch == 'on') {
                            //缓存
                            previousValue = await cacheHandlers.getCache(cacheKey);
                        }
                        if (!previousValue) {
                            now = Date.now();
                            let retryCountThree = 0;
                            let resultPromise = null;
                            do {
                                try {
                                    resultPromise = await executeQuery(queryBody);
                                    break;
                                } catch (err) {
                                    retryCountThree++;
                                    console.error(err);
                                    logger.error(err);
                                }
                            } while (retryCountThree < 3)
                            if (retryCountThree == 3) {
                                console.error('executeQuery execute fail after trying 3 times: ' + queryBody);
                                logger.error('executeQuery execute fail after trying 3 times: ' + queryBody);
                            }
                            console.log('query arangodb server: ' + queryBody);
                            logger.info('query arangodb server: ' + queryBody);
                            let investPathQueryCost = Date.now() - now;
                            logger.info(`from: ${from} to: ${to}` + " InvestPathQueryCost: " + investPathQueryCost + 'ms');
                            console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ` from: ${from} to: ${to}` + ", InvestPathQueryCost: " + investPathQueryCost + 'ms');
                            if (resultPromise.length > 0) {
                                let result = await handlerQueryResult2(from, to, resultPromise, j);
                                if (cacheSwitch == 'on') {
                                    cacheHandlers.setCache(cacheKey, JSON.stringify(result));
                                }
                                if (warmUpSwitch == 'on') {
                                    //根据预热条件的阈值判断是否要加入预热
                                    let queryCostUp = config.warmUp_Condition.queryCost;
                                    let recordsUp = config.warmUp_Condition.queryRecords;
                                    if (investPathQueryCost >= queryCostUp || resultPromise.length >= recordsUp) {
                                        let conditionsKey = config.redisKeyName.conditionsKey;
                                        let conditionsField = [from, to].join('->');
                                        let conditionsValue = { from: from, to: to, depth: [IVDepth], relations: [j], cost: investPathQueryCost };
                                        cacheHandlers.setWarmUpConditionsToRedis(conditionsKey, conditionsField, JSON.stringify(conditionsValue));
                                        cacheHandlers.setWarmUpPathsToRedis(warmUpKey, JSON.stringify(result));
                                    }
                                }
                                return resolve(result);
                            }
                            else if (resultPromise.records.length == 0) {
                                let nodeResults = {
                                    nodeResultOne: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${j}`, typeName: 'InvestPath', names: [], codes: [] } },
                                    nodeResultTwo: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_10`, typeName: 'guarantees', names: [], codes: [] } },
                                    nodeResultThree: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_11`, typeName: 'family', names: [], codes: [] } }
                                };
                                if (cacheSwitch == 'on') {
                                    cacheHandlers.setCache(cacheKey, JSON.stringify(nodeResults));
                                }
                                return resolve(nodeResults);
                            }
                        }
                        else if (previousValue) {
                            return resolve(JSON.parse(previousValue));
                        }
                    }
                }
                else if (warmUpValue) {
                    warmUpValue = JSON.parse(warmUpValue);
                    console.log('get the warmUpValue from redis, the key is: ' + warmUpKey);
                    logger.info('get the warmUpValue from redis, the key is: ' + warmUpKey);
                    return resolve(warmUpValue);
                }

            } catch (err) {
                console.error(err);
                logger.error(err);
                return reject(err);
            }
        })
    },

    //直接被投资关系的路径查询
    queryInvestedByPath: async function (from, to, IVBDepth, lowWeight, highWeight, pathType) {
        return new Promise(async function (resolve, reject) {

            try {
                let fromIsPerson = 0;
                let toIsPerson = 0;
                if (from.indexOf('P') >= 0) {
                    fromIsPerson = 1;
                }
                if (to.indexOf('P') >= 0) {
                    toIsPerson = 1;
                }
                let j = 1;                                                            //记录每种路径查询方式索引号,investByPathQuery索引为1
                let warmUpKey = [from, to, IVBDepth, j, pathType].join('-');
                let cacheKey = [j, from, to, IVBDepth, lowWeight, highWeight, pathType].join('-');
                let warmUpValue = null;
                if (warmUpSwitch == 'on') {
                    //先从redis中预热的数据中查询是否存在预热值
                    warmUpValue = await cacheHandlers.getWarmUpPathsFromRedis(warmUpKey);
                }
                if (!warmUpValue) {
                    if (from == null || to == null) {
                        console.error(`${from} or ${to} is not in the database at all !`);
                        logger.error(`${from} or ${to} is not in the database at all !`);
                        let nodeResults = {
                            nodeResultOne: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${j}`, typeName: 'InvestedByPath', names: [], codes: [] } },
                            nodeResultTwo: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_10`, typeName: 'guarantees', names: [], codes: [] } },
                            nodeResultThree: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_11`, typeName: 'family', names: [], codes: [] } }
                        };
                        return resolve(nodeResults);
                    }
                    else if (from != null && to != null) {
                        let queryBody = null;
                        let lowWeightFilter = null;
                        let highWeightFilter = null;
                        let edgeCollectionName = null;
                        if (lowWeight) {
                            lowWeightFilter = `filter e.weight >= ${lowWeight}`;
                        }
                        if (highWeight) {
                            highWeightFilter = `filter e.weight <= ${highWeight}`;
                        }
                        let nodeCollectionName = config.arangodbInfo.nodeCollectionName;
                        let invest = config.arangodbInfo.edgeCollectionName.invests;
                        let family = config.arangodbInfo.edgeCollectionName.family;
                        let guanrantee = config.arangodbInfo.edgeCollectionName.guarantees;
                        let queryFirst =
                                        `
                                        let from = '${nodeCollectionName}/${from}'
                                        let to = '${nodeCollectionName}/${to}'
                                        for v, e in 1..2 inbound from ${invest}
                                            filter v.flag != 1 && e.flag != 1
                                            collect with count into lf
                                        for v, e in 1..2 outbound to ${invest}
                                            filter v.flag != 1 && e.flag != 1
                                            collect with count into lt
                                        let s = (lf <= lt ? '${from}' : '${to}')
                                        let t = (lf > lt ? '${from}' : '${to}')
                                        let direction = (lf <= lt ? 'inbound' : 'outbound')
                                        return {s, t, lf, lt, direction}
                                        `;
                        console.log('queryFirst: ', queryFirst);
                        logger.info('queryFirst: ', queryFirst);
                        let queryFirstStart = Date.now();
                        let retryCount = 0;
                        let resultOne = null;
                        do {
                            try {
                                resultOne = await executeQuery(queryFirst);
                                break;
                            } catch (err) {
                                retryCount++;
                                console.error(err);
                                logger.error(err);
                                return reject(err);
                            }
                        } while (retryCount < 3)
                        if (retryCount == 3) {
                            console.error('queryFirst execute fail after trying 3 times: ' + queryBody);
                            logger.error('queryFirst execute fail after trying 3 times: ' + queryBody);
                        }
                        let queryFirstCost = Date.now() - queryFirstStart;
                        let source = null;
                        let target = null;
                        let direction = null;
                        if (resultOne.length > 0 && resultOne[0].hasOwnProperty('direction')) {
                            source = resultOne[0].s;
                            target = resultOne[0].t;
                            sourceLen = resultOne[0].lf;
                            targetLen = resultOne[0].lt;
                            direction = resultOne[0].direction;
                            console.log(`source: ${source}, sourceLen: ${sourceLen}, target: ${target}, targetLen: ${targetLen}, direction is: ${direction}, queryFirstCost: ${queryFirstCost} ms`);
                            logger.info(`source: ${source}, sourceLen: ${sourceLen}, target: ${target}, targetLen: ${targetLen}, direction is: ${direction}, queryFirstCost: ${queryFirstCost} ms`);
                        }
                        else {
                            console.error('queryFirst failure!');
                            logger.error('queryFirst failure!');
                            return reject(err);
                        }
                        if (pathType == 'invests' || !pathType) {
                            edgeCollectionName = invest;
                        }
                        else if (pathType == 'all') {
                            //from/to均为自然人时只找family关系
                            if (fromIsPerson == 1 && toIsPerson == 1) {
                                edgeCollectionName = family;
                            }
                            else if (fromIsPerson == 1 || toIsPerson == 1) {
                                edgeCollectionName = `${invest}, any ${family}, ${guanrantee}`;
                            }
                            else if (fromIsPerson != 1 && toIsPerson != 1) {
                                edgeCollectionName = `${invest}, ${guanrantee}`;
                            }
                        }
                        queryBody =
                                    `
                                    for source in nodes
                                        filter source.flag != 1
                                        filter source._key == '${source}'
                                        limit 1
                                    for v, e, p
                                        in 1..${IVBDepth} ${direction} source
                                        ${edgeCollectionName}
                                        filter v.flag != 1 && e.flag != 1
                                        filter p.edges[*].flag all != 1
                                        filter v._key == '${target}'
                                        ${lowWeightFilter}
                                        ${highWeightFilter}
                                        return distinct p
                                    `;
                        queryBody = queryBody.replace(/null/g, '');             

                        let now = 0;
                        let previousValue = null;
                        if (cacheSwitch == 'on') {
                            //缓存
                            previousValue = await cacheHandlers.getCache(cacheKey);
                        }
                        if (!previousValue) {
                            now = Date.now();
                            let retryCountThree = 0;
                            let resultPromise = null;
                            do {
                                try {
                                    resultPromise = await executeQuery(queryBody);
                                    break;
                                } catch (err) {
                                    retryCountThree++;
                                    console.error(err);
                                    logger.error(err);
                                }
                            } while (retryCountThree < 3)
                            if (retryCountThree == 3) {
                                console.error('executeQuery execute fail after trying 3 times: ' + queryBody);
                                logger.error('executeQuery execute fail after trying 3 times: ' + queryBody);
                            }
                            console.log('query arangodb server: ' + queryBody);
                            logger.info('query arangodb server: ' + queryBody);
                            let investedByPathQueryCost = Date.now() - now;
                            logger.info(`from: ${from} to: ${to}` + " InvestedByPathQueryCost: " + investedByPathQueryCost + 'ms');
                            console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ` from: ${from} to: ${to}` + ", InvestedByPathQueryCost: " + investedByPathQueryCost + 'ms');
                            if (resultPromise.length > 0) {
                                let result = await handlerQueryResult2(from, to, resultPromise, j);
                                if (cacheSwitch == 'on') {
                                    cacheHandlers.setCache(cacheKey, JSON.stringify(result));
                                }
                                if (warmUpSwitch == 'on') {
                                    //根据预热条件的阈值判断是否要加入预热
                                    let queryCostUp = config.warmUp_Condition.queryCost;
                                    let recordsUp = config.warmUp_Condition.queryRecords;
                                    if (investedByPathQueryCost >= queryCostUp || resultPromise.length >= recordsUp) {
                                        let conditionsKey = config.redisKeyName.conditionsKey;
                                        let conditionsField = [from, to].join('->');
                                        let conditionsValue = { from: from, to: to, depth: [IVBDepth], relations: [j], cost: investedByPathQueryCost };
                                        cacheHandlers.setWarmUpConditionsToRedis(conditionsKey, conditionsField, JSON.stringify(conditionsValue));
                                        cacheHandlers.setWarmUpPathsToRedis(warmUpKey, JSON.stringify(result));
                                    }
                                }
                                return resolve(result);
                            }
                            else if (resultPromise.length == 0) {
                                let nodeResults = {
                                    nodeResultOne: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${j}`, typeName: 'InvestedByPath', names: [], codes: [] } },
                                    nodeResultTwo: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_10`, typeName: 'guarantees', names: [], codes: [] } },
                                    nodeResultThree: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_11`, typeName: 'family', names: [], codes: [] } }
                                };
                                if (cacheSwitch == 'on') {
                                    cacheHandlers.setCache(cacheKey, JSON.stringify(nodeResults));
                                }
                                return resolve(nodeResults);
                            }
                        }
                        else if (previousValue) {
                            return resolve(JSON.parse(previousValue));
                        }
                    }
                }
                else if (warmUpValue) {
                    warmUpValue = JSON.parse(warmUpValue);
                    console.log('get the warmUpValue from redis, the key is: ' + warmUpKey);
                    logger.info('get the warmUpValue from redis, the key is: ' + warmUpKey);
                    return resolve(warmUpValue);
                }
            } catch (err) {
                console.error(err);
                logger.error(err);
                return reject(err);
            }
        })
    },

    //共同投资关系路径查询
    queryCommonInvestPath: async function (from, to, CIVDepth, lowWeight, highWeight, pathType) {
        return new Promise(async function (resolve, reject) {

            try {
                let j = 4;
                let warmUpKey = [from, to, CIVDepth, j, pathType].join('-');
                let cacheKey = [j, from, to, CIVDepth, lowWeight, highWeight, pathType].join('-');
                let warmUpValue = null;
                if (warmUpSwitch == 'on') {
                    //先从redis中预热的数据中查询是否存在预热值
                    warmUpValue = await cacheHandlers.getWarmUpPathsFromRedis(warmUpKey);
                }
                if (!warmUpValue) {
                    if (from == null || to == null) {
                        console.error(`${from} or ${to} is not in the database at all !`);
                        logger.error(`${from} or ${to} is not in the database at all !`);
                        let nodeResults = {
                            nodeResultOne: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${j}`, typeName: 'CommonInvestPath', names: [], codes: [] } },
                            nodeResultTwo: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_10`, typeName: 'guarantees', names: [], codes: [] } },
                            nodeResultThree: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_11`, typeName: 'family', names: [], codes: [] } }
                        };
                        return resolve(nodeResults);
                    }
                    else if (from != null && to != null) {
                        let lowWeightFilter = null;
                        let highWeightFilter = null;
                        let edgeCollectionName = null;
                        if (lowWeight) {
                            lowWeightFilter = `filter e.weight >= ${lowWeight}`;
                        }
                        if (highWeight) {
                            highWeightFilter = `filter e.weight <= ${highWeight}`;
                        }
                        let invest = config.arangodbInfo.edgeCollectionName.invests;
                        let family = config.arangodbInfo.edgeCollectionName.family;
                        let guanrantee = config.arangodbInfo.edgeCollectionName.guarantees;
                        if (pathType == 'invests' || !pathType) {
                            edgeCollectionName = invest;
                        }
                        else if (pathType == 'all') {
                            edgeCollectionName = `${invest}, ${guanrantee}, any ${family}`;
                        }
                        let queryBody =
                                        `
                                        let from = 'nodes/${from}'
                                        let to = 'nodes/${to}'
                                        for v1, e1, p1 in 1..${CIVDepth/2} outbound from
                                            ${edgeCollectionName}
                                            filter v1.flag != 1 && e1.flag != 1
                                            filter p1.edges[*].flag all != 1
                                            ${lowWeightFilter}
                                            ${highWeightFilter}
                                            for v2, e2, p2 in 1..${CIVDepth/2} outbound to
                                                ${edgeCollectionName}
                                                filter v2.flag != 1 && e2.flag != 1
                                                filter p2.edges[*].flag all != 1
                                                ${lowWeightFilter}
                                                ${highWeightFilter}
                                                filter e1._to == e2._to
                                                return distinct {'edges': append(p1.edges, p2.edges), 'vertices': append(p1.vertices, p2.vertices)}                                        `;
                        queryBody = queryBody.replace(/null/g, '');  
                        let now = 0;
                        let previousValue = null;
                        if (cacheSwitch == 'on') {
                            //缓存
                            previousValue = await cacheHandlers.getCache(cacheKey);
                        }
                        if (!previousValue) {
                            let commonInvestPathQueryCost = 0;
                            now = Date.now();
                            let retryCountThree = 0;
                            let resultPromise = null;
                            do {
                                try {
                                    resultPromise = await executeQuery(queryBody);
                                    break;
                                } catch (err) {
                                    retryCountThree++;
                                    console.error(err);
                                    logger.error(err);
                                }
                            } while (retryCountThree < 3)
                            if (retryCountThree == 3) {
                                console.error('executeQuery execute fail after trying 3 times: ' + queryBody);
                                logger.error('executeQuery execute fail after trying 3 times: ' + queryBody);
                            }
                            console.log('query arangodb server: ' + queryBody);
                            logger.info('query arangodb server: ' + queryBody);
                            commonInvestPathQueryCost = Date.now() - now;
                            logger.info(`from: ${from} to: ${to}` + " CommonInvestPathQueryCost: " + commonInvestPathQueryCost + 'ms');
                            console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ` from: ${from} to: ${to}` + ", CommonInvestPathQueryCost: " + commonInvestPathQueryCost + 'ms');
                            if (resultPromise.length > 0) {
                                logger.info(`from: ${from} to: ${to}` + " CommonInvestPathQueryCost: " + commonInvestPathQueryCost + 'ms');
                                let result = await handlerQueryResult2(from, to, resultPromise, j);
                                if (cacheSwitch == 'on') {
                                    cacheHandlers.setCache(cacheKey, JSON.stringify(result));
                                }
                                if (warmUpSwitch == 'on') {
                                    //根据预热条件的阈值判断是否要加入预热
                                    let queryCostUp = config.warmUp_Condition.queryCost;
                                    let recordsUp = config.warmUp_Condition.queryRecords;
                                    if (commonInvestPathQueryCost >= queryCostUp || resultPromise.length >= recordsUp) {
                                        let conditionsKey = config.redisKeyName.conditionsKey;
                                        let conditionsField = [from, to].join('->');
                                        let conditionsValue = { from: from, to: to, depth: [CIVDepth], relations: [j], cost: commonInvestPathQueryCost };
                                        cacheHandlers.setWarmUpConditionsToRedis(conditionsKey, conditionsField, JSON.stringify(conditionsValue));
                                        cacheHandlers.setWarmUpPathsToRedis(warmUpKey, JSON.stringify(result));
                                    }
                                }
                                return resolve(result);
                            } else if (resultPromise.records.length == 0) {
                                let nodeResults = {
                                    nodeResultOne: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${j}`, typeName: 'CommonInvestPath', names: [], codes: [] } },
                                    nodeResultTwo: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_10`, typeName: 'guarantees', names: [], codes: [] } },
                                    nodeResultThree: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_11`, typeName: 'family', names: [], codes: [] } }
                                };
                                if (cacheSwitch == 'on') {
                                    cacheHandlers.setCache(cacheKey, JSON.stringify(nodeResults));
                                }
                                return resolve(nodeResults);
                            }
                        }
                        else if (previousValue) {
                            return resolve(JSON.parse(previousValue));
                        }
                    }
                }
                else if (warmUpValue) {
                    warmUpValue = JSON.parse(warmUpValue);
                    console.log('get the warmUpValue from redis, the key is: ' + warmUpKey);
                    logger.info('get the warmUpValue from redis, the key is: ' + warmUpKey);
                    return resolve(warmUpValue);
                }
            } catch (err) {
                console.error(err);
                logger.error(err);
                return reject(err);
            }
        })
    },

    //共同被投资关系路径查询
    queryCommonInvestedByPath: async function (from, to, CIVBDepth, lowWeight, highWeight, isExtra, pathType) {
        return new Promise(async function (resolve, reject) {

            try {
                let j = 5;
                //先从redis中预热的数据中查询是否存在预热值
                let warmUpKey = [from, to, CIVBDepth, j, isExtra, pathType].join('-');
                let cacheKey = [j, from, to, CIVBDepth, lowWeight, highWeight, isExtra, pathType].join('-');
                let warmUpValue = null;
                if (warmUpSwitch == 'on') {
                    warmUpValue = await cacheHandlers.getWarmUpPathsFromRedis(warmUpKey);
                }
                if (!warmUpValue) {
                    if (from == null || to == null) {
                        console.error(`${from} or ${to} is not in the neo4j database at all !`);
                        logger.error(`${from} or ${to} is not in the neo4j database at all !`);
                        let nodeResults = {
                            nodeResultOne: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${j}`, typeName: 'CommonInvestedByPath', names: [], codes: [] } },
                            nodeResultTwo: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_10`, typeName: 'guarantees', names: [], codes: [] } },
                            nodeResultThree: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_11`, typeName: 'family', names: [], codes: [] } }
                        };
                        return resolve(nodeResults);
                    }
                    else if (from != null && to != null) {
                        let lowWeightFilter = null;
                        let highWeightFilter = null;
                        let edgeCollectionName = null;
                        let isExtraFilter = null;
                        if (isExtra == 0 || isExtra == '0') {
                            isExtraFilter = 'filter v.isExtra == 0';
                        }
                        if (lowWeight) {
                            lowWeightFilter = `filter e.weight >= ${lowWeight}`;
                        }
                        if (highWeight) {
                            highWeightFilter = `filter e.weight <= ${highWeight}`;
                        }
                        let invest = config.arangodbInfo.edgeCollectionName.invests;
                        let family = config.arangodbInfo.edgeCollectionName.family;
                        let guanrantee = config.arangodbInfo.edgeCollectionName.guarantees;
                        if (pathType == 'invests' || !pathType) {
                            edgeCollectionName = invest;
                        }
                        else if (pathType == 'all') {
                            edgeCollectionName = `${invest}, ${guanrantee}, any ${family}`;
                        }
                        let queryBody =
                                        `
                                        let from = 'nodes/${from}'
                                        let to = 'nodes/${to}'
                                        for v1, e1, p1 in 1..${CIVBDepth/2} inbound from
                                            ${edgeCollectionName}
                                            filter v1.flag != 1 && e1.flag != 1
                                            filter p1.edges[*].flag all != 1
                                            ${isExtraFilter}
                                            ${lowWeightFilter}
                                            ${highWeightFilter}
                                            for v2, e2, p2 in 1..${CIVBDepth/2} inbound to
                                                ${edgeCollectionName}
                                                filter v2.flag != 1 && e2.flag != 1
                                                filter p2.edges[*].flag all != 1
                                                ${isExtraFilter}
                                                ${lowWeightFilter}
                                                ${highWeightFilter}
                                                filter e1._from == e2._from
                                                return distinct {'edges': append(p1.edges, p2.edges), 'vertices': append(p1.vertices, p2.vertices)}                                        `;
                        queryBody = queryBody.replace(/null/g, '');  
                        let now = 0;
                        let previousValue = null;
                        if (cacheSwitch == 'on') {
                            //缓存
                            previousValue = await cacheHandlers.getCache(cacheKey);
                        }
                        if (!previousValue) {
                            let commonInvestedByPathQueryCost = 0;
                            now = Date.now();
                            let retryCountThree = 0;
                            let resultPromise = null;
                            do {
                                try {
                                    resultPromise = await executeQuery(queryBody);
                                    break;
                                } catch (err) {
                                    retryCountThree++;
                                    console.error(err);
                                    logger.error(err);
                                }
                            } while (retryCountThree < 3)
                            if (retryCountThree == 3) {
                                console.error('executeQuery execute fail after trying 3 times: ' + queryBody);
                                logger.error('executeQuery execute fail after trying 3 times: ' + queryBody);
                            }
                            console.log('query arangodb server: ' + queryBody);
                            logger.info('query arangodb server: ' + queryBody);
                            commonInvestedByPathQueryCost = Date.now() - now;
                            logger.info(`from: ${from} to: ${to}` + " CommonInvestedByPathQueryCost: " + commonInvestedByPathQueryCost + 'ms');
                            console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ` from: ${from} to: ${to}` + ", CommonInvestedByPathQueryCost: " + commonInvestedByPathQueryCost + 'ms');
                            if (resultPromise.length > 0) {
                                let result = await handlerQueryResult2(from, to, resultPromise, j);
                                if (cacheSwitch == 'on') {
                                    cacheHandlers.setCache(cacheKey, JSON.stringify(result));
                                }
                                if (warmUpSwitch == 'on') {
                                    //根据预热条件的阈值判断是否要加入预热
                                    let queryCostUp = config.warmUp_Condition.queryNeo4jCost;
                                    let recordsUp = config.warmUp_Condition.queryNeo4jRecords;
                                    if (commonInvestedByPathQueryCost >= queryCostUp || resultPromise.records.length >= recordsUp) {
                                        let conditionsKey = config.redisKeyName.conditionsKey;
                                        let conditionsField = [from, to].join('->');
                                        let conditionsValue = { from: from, to: to, depth: [CIVBDepth], relations: [j], cost: commonInvestedByPathQueryCost };
                                        cacheHandlers.setWarmUpConditionsToRedis(conditionsKey, conditionsField, JSON.stringify(conditionsValue));
                                        cacheHandlers.setWarmUpPathsToRedis(warmUpKey, JSON.stringify(result));
                                    }
                                }
                                return resolve(result);
                            } else if (resultPromise.length == 0) {
                                let nodeResults = {
                                    nodeResultOne: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${j}`, typeName: 'CommonInvestedByPath', names: [], codes: [] } },
                                    nodeResultTwo: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_10`, typeName: 'guarantees', names: [], codes: [] } },
                                    nodeResultThree: { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_11`, typeName: 'family', names: [], codes: [] } }
                                };
                                if (cacheSwitch == 'on') {
                                    cacheHandlers.setCache(cacheKey, JSON.stringify(nodeResults));
                                }
                                return resolve(nodeResults);
                            }
                        }
                        else if (previousValue) {
                            return resolve(JSON.parse(previousValue));
                        }
                    }
                }
                else if (warmUpValue) {
                    warmUpValue = JSON.parse(warmUpValue);
                    console.log('get the warmUpValue from redis, the key is: ' + warmUpKey);
                    logger.info('get the warmUpValue from redis, the key is: ' + warmUpKey);
                    return resolve(warmUpValue);
                }
            } catch (err) {
                console.error(err);
                logger.error(err);
                return reject(err);
            }
        })
    },

    //高管投资关系路径查询
    queryExecutiveInvestPath: async function (code, surStatus) {
        return new Promise(async function (resolve, reject) {

            try {
                let now = Date.now();
                let j = 12;                                                              //记录每种路径查询方式索引号,directInvestPathQuery索引为6
                let pathType = 'executes';
                let cacheKey = [j, code, pathType].join('-');
                let previousValue = null;
                if (cacheSwitch == 'on') {
                    //缓存
                    previousValue = await cacheHandlers.getCache(cacheKey);
                }
                if (!previousValue) {
                    let queryBody = null;
                    let surStatusFilter = null;
                    let nodeCollectionName = config.arangodbInfo.nodeCollectionName;
                    let edgeCollectionName = config.arangodbInfo.edgeCollectionName[`${pathType}`];
                    if (surStatus == 1 || surStatus == '1') {
                        surStatusFilter = 'filter v.surStatus == 1';
                    }
                    queryBody = 
                                `for v, e, p in 1..1 outbound '${nodeCollectionName}/${code}' ${edgeCollectionName}
                                    filter v.flag != 1 && e.flag != 1
                                    ${surStatusFilter}
                                    return distinct p`;
                    queryBody = queryBody.replace(/null/g, '');
                    let retryCount = 0;
                    let resultPromise = null;
                    do {
                        try {
                            resultPromise = await executeQuery(queryBody);
                            break;
                        } catch (err) {
                            retryCount++;
                            console.error(err);
                            logger.error(err);
                        }
                    } while (retryCount < 3)
                    if (retryCount == 3) {
                        console.error('executeQuery execute fail after trying 3 times: ' + queryBody);
                        logger.error('executeQuery execute fail after trying 3 times: ' + queryBody);
                    }
                    console.log('query arangodb server: ' + queryBody);
                    logger.info('query arangodb server: ' + queryBody);
                    let executiveInvestPathQueryCost = Date.now() - now;
                    logger.info(`code: ${code}` + " executiveInvestPathQueryCost: " + executiveInvestPathQueryCost + 'ms');
                    console.log("time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ` code: ${code} ` + ",  executiveInvestPathQueryCost: " + executiveInvestPathQueryCost + 'ms');
                    if (resultPromise.length > 0) {
                        let result = await resultHandlers.handlerPromise2(null, null, resultPromise, j);
                        let afterPathDetailFour = setSourceTarget2(result.pathTypeFour.mapRes, result.pathTypeFour.dataDetail.data.pathDetail);
                        let sortPathDetailFour = afterPathDetailFour.sort(sortRegName);                                   //按注册资本、名称排序
                        result.pathTypeFour.dataDetail.data.pathDetail = sortPathDetailFour;
                        let newResult = result.pathTypeFour.dataDetail;
                        if (cacheSwitch == 'on') {
                            cacheHandlers.setCache(cacheKey, JSON.stringify(newResult));
                        }
                        return resolve(newResult);
                    }
                    else {
                        return resolve({ data: { pathDetail: [], pathNum: 0 }, type: `result_12`, typeName: 'executiveInvestPath', names: [] });
                    }
                }
                else if (previousValue) {
                    return resolve(JSON.parse(previousValue));
                }
            }
            catch (err) {
                console.error(err);
                logger.error(err);
                return reject(err);
            }
        })
    },

    //担保关系的路径查询
    queryGuaranteePath: async function (from, to, GTDepth) {
        return new Promise(async function (resolve, reject) {

            try {
                let j = 8;                                                                                   //记录每种路径查询方式索引号
                //先从redis中预热的数据中查询是否存在预热值
                let warmUpKey = [from, to, GTDepth, j].join('-');
                let cacheKey = [j, from, to, GTDepth, 'GT'].join('-');
                let warmUpValue = null;
                if (warmUpSwitch == 'on') {
                    warmUpValue = await cacheHandlers.getWarmUpPathsFromRedis(warmUpKey);
                }
                if (!warmUpValue) {
                    if (from == null || to == null) {
                        console.error(`${from} or ${to} is not in the database at all !`);
                        logger.error(`${from} or ${to} is not in the database at all !`);
                        let nodeResults = { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${j}`, typeName: 'GuaranteePath', names: [], codes: [] } };
                        return resolve(nodeResults);
                    }
                    else if (from != null && to != null) {
                        let queryBody = null;
                        let nodeCollectionName = config.arangodbInfo.nodeCollectionName;
                        let guanrantee = config.arangodbInfo.edgeCollectionName.guarantees;
                        let queryFirst =
                                        `
                                        let from = '${nodeCollectionName}/${from}'
                                        let to = '${nodeCollectionName}/${to}'
                                        for v, e in 1..2 outbound from ${guanrantee}
                                            filter v.flag != 1 && e.flag != 1
                                            collect with count into lf
                                        for v, e in 1..2 inbound to ${guanrantee}
                                            filter v.flag != 1 && e.flag != 1
                                            collect with count into lt
                                        let s = (lf <= lt ? '${from}' : '${to}')
                                        let t = (lf > lt ? '${from}' : '${to}')
                                        let direction = (lf <= lt ? 'outbound' : 'inbound')
                                        return {s, t, lf, lt, direction}
                                        `;
                        console.log('queryFirst: ', queryFirst);
                        logger.info('queryFirst: ', queryFirst);
                        let queryFirstStart = Date.now();
                        let retryCount = 0;
                        let resultOne = null;
                        do {
                            try {
                                resultOne = await executeQuery(queryFirst);
                                break;
                            } catch (err) {
                                retryCount++;
                                console.error(err);
                                logger.error(err);
                                return reject(err);
                            }
                        } while (retryCount < 3)
                        if (retryCount == 3) {
                            console.error('queryFirst execute fail after trying 3 times: ' + queryBody);
                            logger.error('queryFirst execute fail after trying 3 times: ' + queryBody);
                        }
                        let queryFirstCost = Date.now() - queryFirstStart;
                        let source = null;
                        let target = null;
                        let direction = null;
                        if (resultOne.length > 0 && resultOne[0].hasOwnProperty('direction')) {
                            source = resultOne[0].s;
                            target = resultOne[0].t;
                            sourceLen = resultOne[0].lf;
                            targetLen = resultOne[0].lt;
                            direction = resultOne[0].direction;
                            console.log(`source: ${source}, sourceLen: ${sourceLen}, target: ${target}, targetLen: ${targetLen}, direction is: ${direction}, queryFirstCost: ${queryFirstCost} ms`);
                            logger.info(`source: ${source}, sourceLen: ${sourceLen}, target: ${target}, targetLen: ${targetLen}, direction is: ${direction}, queryFirstCost: ${queryFirstCost} ms`);
                        }
                        else {
                            console.error('queryFirst failure!');
                            logger.error('queryFirst failure!');
                            return reject(err);
                        }
                        queryBody =
                                    `
                                    for source in nodes
                                        filter source.flag != 1
                                        filter source._key == '${source}'
                                        limit 1
                                    for v, e, p
                                        in 1..${GTDepth} ${direction} source
                                        ${guanrantee}
                                        filter v.flag != 1 && e.flag != 1
                                        filter p.edges[*].flag all != 1
                                        filter v._key == '${target}'
                                        return distinct p
                                    `;
                        queryBody = queryBody.replace(/null/g, '');
                        let now = 0;
                        let previousValue = null;
                        if (cacheSwitch == 'on') {
                            //缓存
                            previousValue = await cacheHandlers.getCache(cacheKey);
                        }
                        if (!previousValue) {
                            let resultPromise = null;
                            now = Date.now();
                            let retryCountThree = 0;
                            do {
                                try {
                                    resultPromise = await executeQuery(queryBody);
                                    break;
                                } catch (err) {
                                    retryCountThree++;
                                    console.error(err);
                                    logger.error(err);
                                }
                            } while (retryCountThree < 3)
                            if (retryCountThree == 3) {
                                console.error('executeQuery execute fail after trying 3 times: ' + queryBody);
                                logger.error('executeQuery execute fail after trying 3 times: ' + guaranteePathQuery);
                            }
                            console.log('query arangodb server: ' + queryBody);
                            logger.info('query arangodb server: ' + queryBody);
                            let guaranteePathQueryCost = Date.now() - now;
                            logger.info(`from: ${from} to: ${to}` + " guaranteePathQueryCost: " + guaranteePathQueryCost + 'ms');
                            console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + `from: ${from} to: ${to}` + ", guaranteePathQueryCost: " + guaranteePathQueryCost + 'ms');
                            if (resultPromise.length > 0) {
                                let result = await handlerQueryResult2(from, to, resultPromise, j);
                                if (cacheSwitch == 'on') {
                                    cacheHandlers.setCache(cacheKey, JSON.stringify(result));
                                }
                                if (warmUpSwitch == 'on') {
                                    //根据预热条件的阈值判断是否要加入预热
                                    let queryCostUp = config.warmUp_Condition.queryNeo4jCost;
                                    let recordsUp = config.warmUp_Condition.queryNeo4jRecords;
                                    if (guaranteePathQueryCost >= queryCostUp || resultPromise.length >= recordsUp) {
                                        let conditionsKey = config.redisKeyName.conditionsKey;
                                        let conditionsField = [from, to].join('->');
                                        let conditionsValue = { from: from, to: to, depth: [GTDepth], relations: [j], cost: guaranteePathQueryCost };
                                        cacheHandlers.setWarmUpConditionsToRedis(conditionsKey, conditionsField, JSON.stringify(conditionsValue));
                                        cacheHandlers.setWarmUpPathsToRedis(warmUpKey, JSON.stringify(result));
                                    }
                                }
                                return resolve(result);
                            }
                            else if (resultPromise.records.length == 0) {
                                let nodeResults = { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${j}`, typeName: 'GuaranteePath', names: [], codes: [] } };
                                if (cacheSwitch == 'on') {
                                    cacheHandlers.setCache(cacheKey, JSON.stringify(nodeResults));
                                }
                                return resolve(nodeResults);
                            }
                        }
                        else if (previousValue) {
                            return resolve(JSON.parse(previousValue));
                        }
                    }
                }
                else if (warmUpValue) {
                    warmUpValue = JSON.parse(warmUpValue);
                    console.log('get the warmUpValue from redis, the key is: ' + warmUpKey);
                    logger.info('get the warmUpValue from redis, the key is: ' + warmUpKey);
                    return resolve(warmUpValue);
                }
            } catch (err) {
                console.error(err);
                logger.error(err);
                return reject(err);
            }
        }) 
    },

    //直接被投资关系的路径查询
    queryGuaranteedByPath: async function (from, to, GTBDepth) {
        return new Promise(async function (resolve, reject) {

            try {
                let j = 9;                                                            //记录每种路径查询方式索引号
                //先从redis中预热的数据中查询是否存在预热值
                let warmUpKey = [from, to, GTBDepth, j].join('-');
                let warmUpValue = await cacheHandlers.getWarmUpPathsFromRedis(warmUpKey);
                let cacheKey = [j, from, to, GTBDepth, 'GTB'].join('-');
                if (!warmUpValue) {
                    if (from == null || to == null) {
                        console.error(`${from} or ${to} is not in the neo4j database at all !`);
                        logger.error(`${from} or ${to} is not in the neo4j database at all !`);
                        // return resolve({ error: `${from}=nodeIdOne ${to}=nodeIdTwo` });
                        let nodeResults = { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${j}`, typeName: 'GuaranteedByPath', names: [], codes: [] } };
                        return resolve(nodeResults);
                    }
                    else if (from != null && to != null) {
                        let queryBody = null;
                        let nodeCollectionName = config.arangodbInfo.nodeCollectionName;
                        let guanrantee = config.arangodbInfo.edgeCollectionName.guarantees;
                        let queryFirst =
                                        `
                                        let from = '${nodeCollectionName}/${from}'
                                        let to = '${nodeCollectionName}/${to}'
                                        for v, e in 1..2 inbound from ${guanrantee}
                                            filter v.flag != 1 && e.flag != 1
                                            collect with count into lf
                                        for v, e in 1..2 outbound to ${guanrantee}
                                            filter v.flag != 1 && e.flag != 1
                                            collect with count into lt
                                        let s = (lf <= lt ? '${from}' : '${to}')
                                        let t = (lf > lt ? '${from}' : '${to}')
                                        let direction = (lf <= lt ? 'inbound' : 'outbound')
                                        return {s, t, lf, lt, direction}
                                        `;
                        console.log('queryFirst: ', queryFirst);
                        logger.info('queryFirst: ', queryFirst);
                        let queryFirstStart = Date.now();
                        let retryCount = 0;
                        let resultOne = null;
                        do {
                            try {
                                resultOne = await executeQuery(queryFirst);
                                break;
                            } catch (err) {
                                retryCount++;
                                console.error(err);
                                logger.error(err);
                                return reject(err);
                            }
                        } while (retryCount < 3)
                        if (retryCount == 3) {
                            console.error('queryFirst execute fail after trying 3 times: ' + queryBody);
                            logger.error('queryFirst execute fail after trying 3 times: ' + queryBody);
                        }
                        let queryFirstCost = Date.now() - queryFirstStart;
                        let source = null;
                        let target = null;
                        let direction = null;
                        if (resultOne.length > 0 && resultOne[0].hasOwnProperty('direction')) {
                            source = resultOne[0].s;
                            target = resultOne[0].t;
                            sourceLen = resultOne[0].lf;
                            targetLen = resultOne[0].lt;
                            direction = resultOne[0].direction;
                            console.log(`source: ${source}, sourceLen: ${sourceLen}, target: ${target}, targetLen: ${targetLen}, direction is: ${direction}, queryFirstCost: ${queryFirstCost} ms`);
                            logger.info(`source: ${source}, sourceLen: ${sourceLen}, target: ${target}, targetLen: ${targetLen}, direction is: ${direction}, queryFirstCost: ${queryFirstCost} ms`);
                        }
                        else {
                            console.error('queryFirst failure!');
                            logger.error('queryFirst failure!');
                            return reject(err);
                        }
                        queryBody =
                                    `
                                    for source in nodes
                                        filter source.flag != 1
                                        filter source._key == '${source}'
                                        limit 1
                                    for v, e, p
                                        in 1..${GTBDepth} ${direction} source
                                        ${guanrantee}
                                        filter v.flag != 1 && e.flag != 1
                                        filter p.edges[*].flag all != 1
                                        filter v._key == '${target}'
                                        return distinct p
                                    `;
                        queryBody = queryBody.replace(/null/g, '');   
                        let previousValue = null;
                        let now = 0;
                        if (cacheSwitch == 'on') {
                            //缓存
                            previousValue = await cacheHandlers.getCache(cacheKey);
                        }
                        if (!previousValue) {
                            let resultPromise = null;
                            now = Date.now();
                            let retryCountThree = 0;
                            do {
                                try {
                                    resultPromise = await executeQuery(queryBody);
                                    break;
                                } catch (err) {
                                    retryCountThree++;
                                    console.error(err);
                                    logger.error(err);
                                }
                            } while (retryCountThree < 3)
                            if (retryCountThree == 3) {
                                console.error('executeQuery execute fail after trying 3 times: ' + queryBody);
                                logger.error('executeQuery execute fail after trying 3 times: ' + queryBody);
                            }
                            console.log('query arangodb server: ' + queryBody);
                            logger.info('query arangodb server: ' + queryBody);
                            let guaranteedByPathQueryCost = Date.now() - now;
                            logger.info(`from: ${from} to: ${to}` + " guaranteedByPathQueryCost: " + guaranteedByPathQueryCost + 'ms');
                            console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + `from: ${from} to: ${to}` + ", guaranteedByPathQueryCost: " + guaranteedByPathQueryCost + 'ms');
                            if (resultPromise.length > 0) {
                                let result = await handlerQueryResult2(from, to, resultPromise, j);
                                if (cacheSwitch == 'on') {
                                    cacheHandlers.setCache(cacheKey, JSON.stringify(result));
                                }
                                if (warmUpSwitch == 'on') {
                                    //根据预热条件的阈值判断是否要加入预热
                                    let queryCostUp = config.warmUp_Condition.queryNeo4jCost;
                                    let recordsUp = config.warmUp_Condition.queryNeo4jRecords;
                                    if (guaranteedByPathQueryCost >= queryCostUp || resultPromise.length >= recordsUp) {
                                        let conditionsKey = config.redisKeyName.conditionsKey;
                                        let conditionsField = [from, to].join('->');
                                        let conditionsValue = { from: from, to: to, depth: [GTBDepth], relations: [j], cost: guaranteedByPathQueryCost };
                                        cacheHandlers.setWarmUpConditionsToRedis(conditionsKey, conditionsField, JSON.stringify(conditionsValue));
                                        cacheHandlers.setWarmUpPathsToRedis(warmUpKey, JSON.stringify(result));
                                    }
                                }
                                return resolve(result);
                            }
                            else if (resultPromise.records.length == 0) {
                                let nodeResults = { pathDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${j}`, typeName: 'guaranteedByPath', names: [], codes: [] } };
                                if (cacheSwitch == 'on') {
                                    cacheHandlers.setCache(cacheKey, JSON.stringify(nodeResults));
                                }
                                return resolve(nodeResults);
                            }
                        }
                        else if (previousValue) {
                            return resolve(JSON.parse(previousValue));
                        }
                    }
                }
                else if (warmUpValue) {
                    warmUpValue = JSON.parse(warmUpValue);
                    console.log('get the warmUpValue from redis, the key is: ' + warmUpKey);
                    logger.info('get the warmUpValue from redis, the key is: ' + warmUpKey);
                    return resolve(warmUpValue);
                }
            } catch (err) {
                console.error(err);
                logger.error(err);
                return reject(err);
            }
        })
    }
}

module.exports = searchGraph;