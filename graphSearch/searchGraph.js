/*
用于企业关联关系信息查询
wrote by tzf, 2017/12/8
*/
const cacheHandlers = require('./cacheHandlers.js');
const pathTreeHandlers = require('./pathTreeHandlers.js');
const req = require('require-yml')
const config = req("config/source.yml");
const Hapi = require('hapi');
const server = new Hapi.Server();
const http = require('http');
const querystring = require('querystring');
const url = require('url');
const express = require('express');
const bodyParser = require('body-parser');
const request = require('request');
const fs = require("fs-extra");
const moment = require('moment');
const log4js = require('log4js');
// const Promise = require('bluebird');
const neo4j = require('neo4j-driver').v1;
const driver = neo4j.driver(`${config.neo4jServer.url}`, neo4j.auth.basic(`${config.neo4jServer.user}`, `${config.neo4jServer.password}`),
    // {
    //     maxConnectionLifetime: 30 * 60 * 60,
    //     maxConnectionPoolSize: 1000,
    //     connectionTimeout: 2 * 60
    // }
);
const lookupTimeout = config.lookupTimeout;
console.log('lookupTimeout: ' + lookupTimeout + 'ms');
const schedule = require("node-schedule");

// let rule = new schedule.RecurrenceRule();
// rule.second = 0;
// schedule.scheduleJob(rule, async function(){
//     let result = await callNeo4jServer();
//    console.log(moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") +' callNeo4jServerResult ITCode: ' +result);
// });

const errorCode = {
    ARG_ERROR: {
        code: -100,
        msg: "请求参数错误"
    },
    NOTSUPPORT: {
        code: -101,
        msg: "不支持的参数"
    },
    INTERNALERROR: {
        code: -200,
        msg: "服务内部错误"
    },
    NOMATCHINGDATA: {
        code: -505,
        msg: "无匹配数据"
    }
}

function errorResp(err, msg) {
    return { ok: err.code, error: msg || err.msg };
}

// log4js.configure({
//     appenders: {
//         'out': {
//             type: 'file',         //文件输出
//             filename: 'logs/queryDataInfo.log',
//             maxLogSize: config.logInfo.maxLogSize
//         }
//     },
//     categories: { default: { appenders: ['out'], level: 'info' } }
// });
// const logger = log4js.getLogger();
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
        default: { appenders: ['console', 'log', 'errorFilter'], level: 'info' }
    },
    pm2: true,
    pm2InstanceVar: 'INSTANCE_ID'
});
const logger = log4js.getLogger('graphTree_search_app');

//定时触发请求neo4j server, 保持session的活跃
function callNeo4jServer() {
    return new Promise(async (resolve, reject) => {
        let resultPromise = session.run(`match (n) return n limit 1`);
        resultPromise.then(result => {
            session.close();
            if (result.records.length > 0) {
                let id = result.records[0]._fields[0].properties.ITCode2.low;
                console.log(moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ' 成功请求neo4j server 一次');
                return resolve(id);
            }
            else if (result.records.length == 0) {
                console.logger(moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ' 请求neo4j server 后未获得结果 ');
                return resolve(null);
            }
        }).catch(err => {
            console.error(moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ' callNeo4jServerError: ' + err);
            logger.error('callNeo4jServerError: ' + err);
            return reject(err);
        });
    });
}

//路径数组元素去除出现两次以上的路径
function uniquePath(pathes, codes) {
    let uniqueFlag = true;
    for (let subCode of codes) {
        let codeIndex = 0;
        for (let subPath of pathes) {
            if (subPath.sourceCode == subCode || subPath.targetCode == subCode) codeIndex++;
        }
        if (codeIndex >= 3) {
            uniqueFlag = false;
            return uniqueFlag;
        }
    }
    return uniqueFlag;
}

function findNodeId(code) {
    return new Promise(async (resolve, reject) => {
        // let session = driver.session();
        let resultPromise = session.run(`match (compId:company {ITCode2: ${code}}) return compId`);
        // let resultPromise = session.run(`match (compId:company {ITCode2: '${code}'}) return compId`);
        resultPromise.then(result => {
            session.close();
            if (result.records.length == 0)
                return resolve(null);
            let id = result.records[0]._fields[0].identity.low;
            if (id)
                return resolve(id);
            else
                return resolve(0);
        }).catch(err => {
            console.error('findNodeIdError: ' + err);
            logger.error('findNodeIdError: ' + err);
            session.close();
            driver.close();
            return reject(err);
        });
    });
}

//处理ITCode2查询ITName为空的情况
function handlerAllNames(names) {
    // let newNames = new Set();
    let newNames = [];
    for (let subName of names) {
        if (subName != '') {
            // newNames.add(subName);
            newNames.push(subName);
        }
        else if (subName == '') {
            subName = 'ITName is null!';
            // newNames.add(subName);
            newNames.push(subName);
        }
    }
    // return Array.from(newNames);
    return newNames;
}

//将ITCode2->ITName对放入Map中
function getCodeNameMapping(codes, names) {
    let codeNameMap = new Map();
    if (codes.length == names.length) {
        for (let i = 0; i < codes.length; i++) {
            codeNameMap.set(codes[i], names[i]);
        }
        return codeNameMap;
    }
}

/* 质朴长存法, num：原数字, n：最终生成的数字位数*/
function pad(num, n) {
    var len = num.toString().length;
    while (len < n) {
        num = "0" + num;
        len++;
    }
    return num;
}

//处理neo4j返回的JSON数据格式
async function handlerPromise(result, index) {
    let allNames = [];
    // let allCodes = [];
    let allCodes = new Set();                                    //使用set保证数据的唯一性
    let uniqueCodes = [];                                            //存储唯一性的codes数据元素
    let num = 0;
    // let detail = {};
    // let i = 0;   
    let pathTypeName = "";
    if (index == 0)
        pathTypeName = "InvestPath";
    else if (index == 1)
        pathTypeName = "InvestedByPath";
    else if (index == 2)
        pathTypeName = "ShortestPath";
    else if (index == 3)
        pathTypeName = "FullPath";
    //只有在单独调用接口时会使用到                                     //记录每种路径查询方式下的具体路径数
    else if (index == 4)
        pathTypeName = "CommonInvestPath";
    else if (index == 5)
        pathTypeName = "CommonInvestedByPath";
    else if (index == 6)
        pathTypeName = "DirectInvestPath";
    else if (index == 7)
        pathTypeName = "DirectInvestedByPath";
    let promiseResult = { toPaNum: num, dataDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${index}`, typeName: pathTypeName, names: [] } };

    promiseResult.dataDetail.data.pathNum = result.records.length;
    // num += promiseResult.dataDetail.data.pathNum;
    num += result.records.length;                                  //调用 neo4j 返回的所有路径，包括有重复点的路径

    if (result.records.length == 0)
        return promiseResult;
    else {
        for (let subRecord of result.records) {
            let pathArray = {};
            let uniquePathArray = {};
            let eachPathArray = [];
            let tempPathArray = [];
            for (let subField of subRecord._fields) {
                // let subFieldCodes = [];
                let subFieldCodes = new Set();
                let uniqueFieldCodes = [];
                for (let subSegment of subField.segments) {
                    let startSegmentCode = 0;
                    if (subSegment.start.properties.ITCode2.low) {
                        startSegmentCode = subSegment.start.properties.ITCode2.low;
                    } else if (!subSegment.start.properties.ITCode2.low && subSegment.start.properties.ITCode2) {
                        startSegmentCode = subSegment.start.properties.ITCode2;
                    }
                    let endSegmentCode = 0;
                    if (subSegment.end.properties.ITCode2.low) {
                        endSegmentCode = subSegment.end.properties.ITCode2.low;
                    } else if (!subSegment.end.properties.ITCode2.low && subSegment.end.properties.ITCode2) {
                        endSegmentCode = subSegment.end.properties.ITCode2;
                    }
                    // subFieldCodes.push(startSegmentCode, endSegmentCode);
                    subFieldCodes.add(startSegmentCode);
                    subFieldCodes.add(endSegmentCode);
                }
                // uniqueFieldCodes = unique(subFieldCodes);
                uniqueFieldCodes = Array.from(subFieldCodes);
                for (let subSegment of subField.segments) {
                    // allNames.push(subSegment.start.properties.ITName, subSegment.end.properties.ITName);
                    // allCodes.push(subSegment.start.properties.ITCode2, subSegment.end.properties.ITCode2);
                    let startSegmentLow = subSegment.start.identity.low;
                    // let startSegmentName = subSegment.start.properties.ITName;

                    //取start node的ITCode2
                    // let startSegmentCode = 0;
                    // if (subSegment.start.properties.ITCode2.low) {
                    //     startSegmentCode = subSegment.start.properties.ITCode2.low;
                    // } else if (!subSegment.start.properties.ITCode2.low && subSegment.start.properties.ITCode2) {
                    //     startSegmentCode = subSegment.start.properties.ITCode2;
                    // }

                    //取到start nodes的isPerson属性
                    let startIsPerson = null;
                    if (subSegment.start.properties.isPerson) {
                        startIsPerson = subSegment.start.properties.isPerson;
                    } else if (!subSegment.start.properties.isPerson && subSegment.start.properties.isPerson.low) {
                        startIsPerson = subSegment.start.properties.isPerson.low;
                    }

                    //取到end nodes的isPerson属性
                    let endIsPerson = null;
                    if (subSegment.end.properties.isPerson) {
                        endIsPerson = subSegment.end.properties.isPerson;
                    } else if (!subSegment.end.properties.isPerson && subSegment.end.properties.isPerson.low) {
                        endIsPerson = subSegment.end.properties.isPerson.low;
                    }

                    //通过isPerson为ITCode2取值
                    let startSegmentCode = null;
                    let endSegmentCode = null;
                    if (startIsPerson == '1' || startIsPerson == 1) {
                        let id = subSegment.start.properties.ITCode2.low;
                        startSegmentCode = 'P' + pad(id, 9);
                    } else {
                        startSegmentCode = subSegment.start.properties.ITCode2.low;
                    }
                    if (endIsPerson == '1' || endIsPerson == 1) {
                        let id = subSegment.end.properties.ITCode2.low;
                        endSegmentCode = 'P' + pad(id, 9);
                    } else {
                        endSegmentCode = subSegment.end.properties.ITCode2.low;
                    }

                    //处理无机构代码的start nodes的name问题
                    let startSegmentName = null;
                    if (subSegment.start.properties.isExtra) {
                        if (subSegment.start.properties.isExtra == 1 || subSegment.start.properties.isExtra == '1' || subSegment.start.properties.isExtra == 'null') {
                            startSegmentName = subSegment.start.properties.name;
                        } else if (subSegment.start.properties.isExtra == 0 || subSegment.start.properties.isExtra == '0') {
                            startSegmentName = null;
                            // allCodes.push(`${startSegmentCode}`); 
                            allCodes.add(`${startSegmentCode}`);                                                                         //将有机构代码的ITCode存入数组
                        }
                    } else if (!subSegment.start.properties.isExtra && subSegment.start.properties.isExtra.low) {
                        if (subSegment.start.properties.isExtra.low == 1 || subSegment.start.properties.isExtra.low == '1' || subSegment.start.properties.isExtra == 'null') {
                            startSegmentName = subSegment.start.properties.name;
                        } else if (subSegment.start.properties.isExtra.low == 0 || subSegment.start.properties.isExtra.low == '0') {
                            startSegmentName = null;
                            // allCodes.push(`${startSegmentCode}`);      
                            allCodes.add(`${startSegmentCode}`);                                                                     //将有机构代码的ITCode存入数组
                        }
                    }

                    //取到start nodes的isExtra属性
                    let startIsExtra = null;
                    if (subSegment.start.properties.isExtra) {
                        startIsExtra = subSegment.start.properties.isExtra;
                    } else if (!subSegment.start.properties.isExtra && subSegment.start.properties.isExtra.low) {
                        startIsExtra = subSegment.start.properties.isExtra.low;
                    }

                    let relSegmentStartLow = subSegment.relationship.start.low;

                    //startRegCapitalRMB取值
                    let startRegCapitalRMB = 0;                                                                    //注册资金(RMB)
                    if (subSegment.start.properties.RMBFund) {
                        startRegCapitalRMB = subSegment.start.properties.RMBFund;
                        startRegCapitalRMB = parseFloat(startRegCapitalRMB.toFixed(2));                            //将RMBFund值转换2位小数              
                    }
                    else if (!subSegment.start.properties.RMBFund && subSegment.start.properties.RMBFund.low) {
                        startRegCapitalRMB = subSegment.start.properties.RMBFund.low;
                        startRegCapitalRMB = parseFloat(startRegCapitalRMB.toFixed(2));                            //将RMBFund值转换2位小数                           
                    }

                    //startRegCapital取值
                    let startRegCapital = 0;                                                                       //注册资金(原单位)
                    if (subSegment.start.properties.regFund) {
                        startRegCapital = subSegment.start.properties.regFund;
                        startRegCapital = parseFloat(startRegCapital.toFixed(2));                                  //将RegFund值转换2位小数              
                    }
                    else if (!subSegment.start.properties.regFund && subSegment.start.properties.regFund.low) {
                        startRegCapital = subSegment.start.properties.regFund.low;
                        startRegCapital = parseFloat(startRegCapital.toFixed(2));                                  //将RegFund值转换2位小数                           
                    }

                    //startRegCapitalUnit取值
                    let startRegCapitalUnit = '万人民币元';                                                         //注册资金单位
                    if (subSegment.start.properties.regFundUnit) {
                        startRegCapitalUnit = subSegment.start.properties.regFundUnit;
                    }
                    else if (!subSegment.start.properties.regFundUnit && subSegment.start.properties.regFundUnit.low) {
                        startRegCapitalUnit = subSegment.start.properties.regFundUnit.low;
                    }

                    //处理持股比例shareholdRatio, 认缴金额(RMB)shareholdQuantityRMB, 认缴金额shareholdQuantity, 认缴金额单位shareholdQuantityUnit
                    let relSegmentEndLow = subSegment.relationship.end.low;
                    //持股比例shareholdRatio取值, 如果weight是全量导入的，直接取weight的值；如果weight是增量导入的，需要取到weight节点下的low值
                    let shareholdRatio = 0;                                                                                                //持股比例
                    if (Object.keys(subSegment.relationship.properties).length > 0) {
                        if (subSegment.relationship.properties.weight.low) {
                            shareholdRatio = subSegment.relationship.properties.weight.low;
                            shareholdRatio = parseFloat(shareholdRatio.toFixed(2));                                                       //将weight值转换2位小数
                        }
                        else if (!subSegment.relationship.properties.weight.low && subSegment.relationship.properties.weight) {
                            shareholdRatio = subSegment.relationship.properties.weight;
                            shareholdRatio = parseFloat(shareholdRatio.toFixed(2));                                                       //将weight值转换2位小数
                        }
                    }

                    //认缴金额(RMB)shareholdQuantityRMB取值
                    let shareholdQuantityRMB = 0;                                                                                           //认缴金额(RMB)
                    if (Object.keys(subSegment.relationship.properties).length > 0) {
                        if (subSegment.relationship.properties.subAmountRMB && subSegment.relationship.properties.subAmountRMB.low) {
                            shareholdQuantityRMB = subSegment.relationship.properties.subAmountRMB.low;
                            shareholdQuantityRMB = parseFloat(shareholdQuantityRMB.toFixed(2));                                              //将subAmountRMB值转换2位小数
                        }
                        else if (!subSegment.relationship.properties.subAmountRMB.low && subSegment.relationship.properties.subAmountRMB) {
                            shareholdQuantityRMB = subSegment.relationship.properties.subAmountRMB;
                            shareholdQuantityRMB = parseFloat(shareholdQuantityRMB.toFixed(2));                                              //将subAmountRMB值转换2位小数
                        }
                    }

                    //认缴金额shareholdQuantity取值
                    let shareholdQuantity = 0;                                                                                               //认缴金额
                    if (Object.keys(subSegment.relationship.properties).length > 0) {
                        if (subSegment.relationship.properties.subAmount.low) {
                            shareholdQuantity = subSegment.relationship.properties.subAmount.low;
                            shareholdQuantity = parseFloat(shareholdQuantity.toFixed(2));                                                     //将subAmount值转换2位小数
                        }
                        else if (!subSegment.relationship.properties.subAmount.low && subSegment.relationship.properties.subAmount) {
                            shareholdQuantity = subSegment.relationship.properties.subAmount;
                            shareholdQuantity = parseFloat(shareholdQuantity.toFixed(2));                                                      //将subAmount值转换2位小数
                        }
                    }

                    //认缴金额单位shareholdQuantityUnit取值
                    let shareholdQuantityUnit = '万人民币元';                                                                                    //认缴金额单位
                    if (Object.keys(subSegment.relationship.properties).length > 0) {
                        if (subSegment.relationship.properties.subAmountUnit.low) {
                            shareholdQuantityUnit = subSegment.relationship.properties.subAmountUnit.low;
                        }
                        else if (!subSegment.relationship.properties.subAmountUnit.low && subSegment.relationship.properties.subAmountUnit) {
                            shareholdQuantityUnit = subSegment.relationship.properties.subAmountUnit;
                        }

                    }

                    let endSegmentLow = subSegment.end.identity.low;
                    // let endSegmentName = subSegment.end.properties.ITName;
                    //取到end nodes的ITCode2
                    // let endSegmentCode = 0;
                    // if (subSegment.end.properties.ITCode2.low) {
                    //     endSegmentCode = subSegment.end.properties.ITCode2.low;
                    // } else if (!subSegment.end.properties.ITCode2.low && subSegment.end.properties.ITCode2) {
                    //     endSegmentCode = subSegment.end.properties.ITCode2;
                    // }

                    //处理无机构代码的end nodes的name问题
                    let endSegmentName = null;
                    if (subSegment.end.properties.isExtra) {
                        if (subSegment.end.properties.isExtra == 1 || subSegment.end.properties.isExtra == '1' || subSegment.end.properties.isExtra == 'null') {
                            endSegmentName = subSegment.end.properties.name;
                        } else if (subSegment.end.properties.isExtra == 0 || subSegment.end.properties.isExtra == '0') {
                            endSegmentName = null;
                            // allCodes.push(`${endSegmentCode}`);    
                            allCodes.add(`${endSegmentCode}`);                                                                      //将有机构代码的ITCode存入数组
                        }
                    } else if (!subSegment.end.properties.isExtra && subSegment.end.properties.isExtra.low) {
                        if (subSegment.end.properties.isExtra.low == 1 || subSegment.end.properties.isExtra.low == '1' || subSegment.end.properties.isExtra == 'null') {
                            endSegmentName = subSegment.end.properties.name;
                        } else if (subSegment.end.properties.isExtra.low == 0 || subSegment.end.properties.isExtra.low == '0') {
                            endSegmentName = null;
                            // allCodes.push(`${endSegmentCode}`); 
                            allCodes.add(`${endSegmentCode}`);                                                                        //将有机构代码的ITCode存入数组
                        }
                    }

                    //取到end nodes的isExtra属性
                    let endIsExtra = null;
                    if (subSegment.end.properties.isExtra) {
                        endIsExtra = subSegment.end.properties.isExtra;
                    } else if (!subSegment.end.properties.isExtra && subSegment.end.properties.isExtra.low) {
                        endIsExtra = subSegment.end.properties.isExtra.low;
                    }

                    //endRegCapitalRMB取值
                    let endRegCapitalRMB = 0;                                                                    //注册资金(RMB)
                    if (subSegment.end.properties.RMBFund) {
                        endRegCapitalRMB = subSegment.end.properties.RMBFund;
                        endRegCapitalRMB = parseFloat(endRegCapitalRMB.toFixed(2));                              //将RMBFund值转换2位小数              
                    }
                    else if (!subSegment.end.properties.RMBFund && subSegment.end.properties.RMBFund.low) {
                        endRegCapitalRMB = subSegment.end.properties.RMBFund.low;
                        endRegCapitalRMB = parseFloat(endRegCapitalRMB.toFixed(2));                              //将RMBFund值转换2位小数                           
                    }

                    //endRegCapital取值
                    let endRegCapital = 0;                                                                       //注册资金(原单位)
                    if (subSegment.end.properties.regFund) {
                        endRegCapital = subSegment.end.properties.regFund;
                        endRegCapital = parseFloat(endRegCapital.toFixed(2));                                  //将RegFund值转换2位小数              
                    }
                    else if (!subSegment.end.properties.regFund && subSegment.end.properties.regFund.low) {
                        endRegCapital = subSegment.end.properties.regFund.low;
                        endRegCapital = parseFloat(endRegCapital.toFixed(2));                                  //将RegFund值转换2位小数                           
                    }

                    //endRegCapitalUnit取值
                    let endRegCapitalUnit = '万人民币元';                                                         //注册资金单位
                    if (subSegment.end.properties.regFundUnit) {
                        endRegCapitalUnit = subSegment.end.properties.regFundUnit;
                    }
                    else if (!subSegment.end.properties.regFundUnit && subSegment.end.properties.regFundUnit.low) {
                        endRegCapitalUnit = subSegment.end.properties.regFundUnit.low;
                    }
                    // let startSegmentName = await queryCodeToName(startSegmentCode);                          //不直接获取eno4j中的ITName，而是通过外部接口由ITCode2获取ITName
                    // let endSegmentName = await queryCodeToName(endSegmentCode);
                    // allNames.push(startSegmentName, endSegmentName);

                    //过滤ITCode2为UUID的codes, 即无机构代码的nodes 不参加code->name的字典查找
                    // allCodes.push(`${startSegmentCode}`, `${endSegmentCode}`);
                    // allCodes.add(startSegmentCode);
                    // allCodes.add(endSegmentCode);
                    let path = {};
                    if (relSegmentStartLow == startSegmentLow && relSegmentEndLow == endSegmentLow) {
                        // path.source = startSegmentName;
                        path.sourceCode = startSegmentCode;
                        path.sourceRegCapitalRMB = startRegCapitalRMB;
                        path.sourceRegCapital = startRegCapital;
                        path.sourceRegCapitalUnit = startRegCapitalUnit;
                        path.source = startSegmentName;
                        path.sourceIsExtra = startIsExtra;
                        path.sourceIsPerson = startIsPerson;
                        // path.target = endSegmentName;
                        path.shareholdRatio = shareholdRatio;
                        path.shareholdQuantityRMB = shareholdQuantityRMB;
                        path.shareholdQuantity = shareholdQuantity;
                        path.shareholdQuantityUnit = shareholdQuantityUnit;

                        path.targetCode = endSegmentCode;
                        path.targetRegCapitalRMB = endRegCapitalRMB;
                        path.targetRegCapital = endRegCapital;
                        path.targetRegCapitalUnit = endRegCapitalUnit;
                        path.target = endSegmentName;
                        path.targetIsExtra = endIsExtra;
                        path.targetIsPerson = endIsPerson;
                    } else if (relSegmentStartLow == endSegmentLow && relSegmentEndLow == startSegmentLow) {
                        // path.source = endSegmentName;
                        path.sourceCode = endSegmentCode;
                        path.sourceRegCapitalRMB = endRegCapitalRMB;
                        path.sourceRegCapital = endRegCapital;
                        path.sourceRegCapitalUnit = endRegCapitalUnit;
                        path.source = endSegmentName;
                        path.sourceIsExtra = endIsExtra;
                        path.sourceIsPerson = endIsPerson;
                        // path.target = startSegmentName;
                        path.shareholdRatio = shareholdRatio;
                        path.shareholdQuantityRMB = shareholdQuantityRMB;
                        path.shareholdQuantity = shareholdQuantity;
                        path.shareholdQuantityUnit = shareholdQuantityUnit;

                        path.targetCode = startSegmentCode;
                        path.targetRegCapitalRMB = startRegCapitalRMB;
                        path.targetRegCapital = startRegCapital;
                        path.targetRegCapitalUnit = startRegCapitalUnit;
                        path.target = startSegmentName;
                        path.targetIsExtra = startIsExtra;
                        path.targetIsPerson = startIsPerson;
                    }
                    //去除包含"流通股"的path, 除包含"流通股份有限公司"的path
                    if (path.source != null) {
                        if (path.source.indexOf('流通股') >= 0 && path.source.indexOf('股份有限公司') == -1) {
                            console.log('过滤包含"流通股"的path, source: ' + path.source);
                            logger.info('过滤包含"流通股"的path, source: ' + path.source);
                        }
                        else {
                            tempPathArray.push(path);
                            pathArray.path = tempPathArray;
                        }
                    }
                    else {
                        tempPathArray.push(path);
                        pathArray.path = tempPathArray;
                    }
                    // tempPathArray.push(path);
                    // pathArray.path = tempPathArray;
                }

                let isUniquePath = false;
                if (pathArray.hasOwnProperty('path')) {
                    isUniquePath = uniquePath(pathArray.path, uniqueFieldCodes);
                }
                if (isUniquePath == true) {
                    uniquePathArray = pathArray;
                }
                else if (isUniquePath == false) {
                    // pathNum--;
                    num--;
                }
            }

            if (Object.keys(uniquePathArray).length != 0)
                promiseResult.dataDetail.data.pathDetail.push(uniquePathArray);
            promiseResult.dataDetail.data.pathNum = num;
            // dataResult.data.pathDetail.name = " ";
        }
        // uniqueCodes = unique(allCodes);
        uniqueCodes = Array.from(allCodes);
        // allNames = await cacheHandlers.getAllNames(uniqueCodes);             //ITCode2->ITName
        let retryCount = 0;
        do {
            try {
                allNames = await cacheHandlers.getAllNames(uniqueCodes);             //ITCode2->ITName
                break;
            } catch (err) {
                retryCount++;
            }
        } while (retryCount < 3)
        if (retryCount == 3) {
            console.error('retryCount: 3, 批量查询机构名称失败');
            logger.error('retryCount: 3, 批量查询机构名称失败');
        }
        let newAllNames = handlerAllNames(allNames);                         // 处理ITName为空的情况

        let codeNameMapRes = getCodeNameMapping(uniqueCodes, newAllNames);   //获取ITCode2->ITName的Map
        promiseResult.mapRes = codeNameMapRes;
        promiseResult.uniqueCodes = uniqueCodes;

        promiseResult.dataDetail.names = newAllNames;
        promiseResult.toPaNum = num;
        return promiseResult;
    }
}

//为每个path追加source/target节点，即增加ITName属性
function setSourceTarget(map, pathDetail) {
    // let newPathDetail = [];
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
                    // targetName = subPath.targetIsExtra;
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

//通过注册资本区间值筛选路径， 对外投资、担保
function filterPathAccRegFund1(paths, lowFund, highFund) {
    let newPaths = [];
    try {
        if (lowFund && highFund) {                                                     //lowFund和highFund存在
            for (let subPathDetail of paths) {
                let regFundSet = new Set();                                           //将每个subPathDetail下的regFund存储
                let flag = true;
                for (let i = 0; i < subPathDetail.path.length; i++) {
                    let sourceRegFund = subPathDetail.path[i].sourceRegCapitalRMB;
                    let targetRegFund = subPathDetail.path[i].targetRegCapitalRMB;
                    regFundSet.add(sourceRegFund);
                    regFundSet.add(targetRegFund);
                    if (i == 0) {
                        regFundSet.delete(sourceRegFund);                               //去除第一个元素
                    }
                }
                if (subPathDetail.path.length != 0) {
                    for (let subRegFund of regFundSet) {
                        if (subRegFund < lowFund || subRegFund > highFund) {            //如果subRegFund不在low和hign范围内，则将flag置成false,并停止数组的遍历
                            flag = false;
                            break;
                        } else if (subRegFund >= lowFund && subRegFund <= highFund) {    //如果subRegFund在low和hign范围内，则将flag置成true,并继续数组的遍历
                            continue;
                        }
                    }
                    if (flag == true) newPaths.push(subPathDetail);
                }
            }
        }
        else if (lowFund && !highFund) {                                               //lowFund存在highFund不存在
            for (let subPathDetail of paths) {
                let regFundSet = new Set();                                            //将每个subPathDetail下的regFund存储
                let flag = true;
                for (let i = 0; i < subPathDetail.path.length; i++) {
                    let sourceRegFund = subPathDetail.path[i].sourceRegCapitalRMB;
                    let targetRegFund = subPathDetail.path[i].targetRegCapitalRMB;
                    regFundSet.add(sourceRegFund);
                    regFundSet.add(targetRegFund);
                    if (i == 0) {
                        regFundSet.delete(sourceRegFund);                               //去除第一个元素
                    }
                }
                if (subPathDetail.path.length != 0) {
                    for (let subRegFund of regFundSet) {
                        if (subRegFund < lowFund) {                                    //如果subRegFund>low，则将flag置成false,并停止数组的遍历
                            flag = false;
                            break;
                        } else if (subRegFund >= lowFund) {                            //如果subRegFund<=low，则将flag置成true,并继续数组的遍历
                            continue;
                        }
                    }
                    if (flag == true) newPaths.push(subPathDetail);
                }
            }
        }
        else if (!lowFund && highFund) {                                               //lowFund不存在highFund存在
            for (let subPathDetail of paths) {
                let regFundSet = new Set();                                            //将每个subPathDetail下的regFund存储
                let flag = true;
                for (let i = 0; i < subPathDetail.path.length; i++) {
                    let sourceRegFund = subPathDetail.path[i].sourceRegCapitalRMB;
                    let targetRegFund = subPathDetail.path[i].targetRegCapitalRMB;
                    regFundSet.add(sourceRegFund);
                    regFundSet.add(targetRegFund);
                    if (i == 0) {
                        regFundSet.delete(sourceRegFund);                               //去除第一个元素
                    }
                }
                if (subPathDetail.path.length != 0) {
                    for (let subRegFund of regFundSet) {
                        if (subRegFund > highFund) {                                   //如果subRegFund>hign，则将flag置成false,并停止数组的遍历
                            flag = false;
                            break;
                        } else if (subRegFund <= highFund) {                          //如果subRegFund<=hign，则将flag置成true,并继续数组的遍历
                            continue;
                        }
                    }
                    if (flag == true) newPaths.push(subPathDetail);
                }
            }
        }
    } catch (err) {
        console.error(err);
        logger.error(err);
        return (err);
    }
    return newPaths;
}

//通过注册资本区间值筛选路径， 股东、被担保
function filterPathAccRegFund2(paths, lowFund, highFund) {
    let newPaths = [];
    try {
        if (lowFund && highFund) {                                                     //lowFund和highFund存在
            for (let subPathDetail of paths) {
                let regFundSet = new Set();                                           //将每个subPathDetail下的regFund存储
                let flag = true;
                for (let i = 0; i < subPathDetail.path.length; i++) {
                    let sourceRegFund = subPathDetail.path[i].sourceRegCapitalRMB;
                    let targetRegFund = subPathDetail.path[i].targetRegCapitalRMB;
                    regFundSet.add(sourceRegFund);
                    regFundSet.add(targetRegFund);
                    if (i == 0) {
                        regFundSet.delete(targetRegFund);                               //去除第一个元素
                    }
                }
                if (subPathDetail.path.length != 0) {
                    for (let subRegFund of regFundSet) {
                        if (subRegFund < lowFund || subRegFund > highFund) {            //如果subRegFund不在low和hign范围内，则将flag置成false,并停止数组的遍历
                            flag = false;
                            break;
                        } else if (subRegFund >= lowFund && subRegFund <= highFund) {    //如果subRegFund在low和hign范围内，则将flag置成true,并继续数组的遍历
                            continue;
                        }
                    }
                    if (flag == true) newPaths.push(subPathDetail);
                }
            }
        }
        else if (lowFund && !highFund) {                                               //lowFund存在highFund不存在
            for (let subPathDetail of paths) {
                let regFundSet = new Set();                                            //将每个subPathDetail下的regFund存储
                let flag = true;
                for (let i = 0; i < subPathDetail.path.length; i++) {
                    let sourceRegFund = subPathDetail.path[i].sourceRegCapitalRMB;
                    let targetRegFund = subPathDetail.path[i].targetRegCapitalRMB;
                    regFundSet.add(sourceRegFund);
                    regFundSet.add(targetRegFund);
                    if (i == 0) {
                        regFundSet.delete(targetRegFund);                               //去除第一个元素
                    }
                }
                if (subPathDetail.path.length != 0) {
                    for (let subRegFund of regFundSet) {
                        if (subRegFund < lowFund) {                                    //如果subRegFund>low，则将flag置成false,并停止数组的遍历
                            flag = false;
                            break;
                        } else if (subRegFund >= lowFund) {                            //如果subRegFund<=low，则将flag置成true,并继续数组的遍历
                            continue;
                        }
                    }
                    if (flag == true) newPaths.push(subPathDetail);
                }
            }
        }
        else if (!lowFund && highFund) {                                               //lowFund不存在highFund存在
            for (let subPathDetail of paths) {
                let regFundSet = new Set();                                            //将每个subPathDetail下的regFund存储
                let flag = true;
                for (let i = 0; i < subPathDetail.path.length; i++) {
                    let sourceRegFund = subPathDetail.path[i].sourceRegCapitalRMB;
                    let targetRegFund = subPathDetail.path[i].targetRegCapitalRMB;
                    regFundSet.add(sourceRegFund);
                    regFundSet.add(targetRegFund);
                    if (i == 0) {
                        regFundSet.delete(targetRegFund);                               //去除第一个元素
                    }
                }
                if (subPathDetail.path.length != 0) {
                    for (let subRegFund of regFundSet) {
                        if (subRegFund > highFund) {                                   //如果subRegFund>hign，则将flag置成false,并停止数组的遍历
                            flag = false;
                            break;
                        } else if (subRegFund <= highFund) {                          //如果subRegFund<=hign，则将flag置成true,并继续数组的遍历
                            continue;
                        }
                    }
                    if (flag == true) newPaths.push(subPathDetail);
                }
            }
        }
    } catch (err) {
        console.error(err);
        logger.error(err);
        return (err);
    }
    return newPaths;
}

//处理neo4j server 返回的结果
async function handlerNeo4jResult(resultPromise, lowFund, highFund) {
    let queryNodeResult = {};
    let j = 6;                                                              //记录每种路径查询方式索引号,directInvestPathQuery索引为6
    let handlerPromiseStart = Date.now();
    let res = await handlerPromise(resultPromise, j);
    let handlerPromiseCost = Date.now() - handlerPromiseStart;
    console.log('handlerPromiseCost: ' + handlerPromiseCost + 'ms');
    logger.info('handlerPromiseCost: ' + handlerPromiseCost + 'ms');
    let beforePathDetail = res.dataDetail.data.pathDetail;                  //没有ITName的pathDetail
    let newBeforePathDetail = [];
    if (lowFund || highFund) {
        let now = Date.now();
        newBeforePathDetail = filterPathAccRegFund1(beforePathDetail, lowFund, highFund);

        let filterPathAccRegFundCost = Date.now() - now;
        console.log('filterPathAccRegFundCost: ' + filterPathAccRegFundCost + 'ms');
    }
    else if (!lowFund && !highFund) {
        newBeforePathDetail = beforePathDetail;
    }
    let afterPathDetail = setSourceTarget(res.mapRes, newBeforePathDetail);
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
    res.dataDetail.data.pathNum = newBeforePathDetail.length;
    queryNodeResult.pathDetail = res.dataDetail;
    return queryNodeResult;
}

//处理neo4j session1
function sessionRun1(queryBody) {
    let session = driver.session();
    return session.run(queryBody).then(result => {
        session.close();
        console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ' sessionRun1 session close!');
        return result;
    });
}

//处理neo4j session2
function sessionRun2(queryBody) {
    let session = driver.session();
    return session.run(queryBody).then(result => {
        session.close();
        console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + ' sessionRun2 session close!');
        return result;
    });
}

let searchGraph = {

    //单个企业直接投资关系路径查询
    queryDirectInvestPath: async function (code, relation, DIDepth, lowWeight, highWeight, lowFund, highFund, lowSubAmountRMB, highSubAmountRMB, isExtra, isBranches, surStatus, personalCode) {
        return new Promise(async function (resolve, reject) {

            try {
                // let session = driver.session();
                let j = 0;                                                                                //0代表DirectInvest
                //先从redis中预热的数据中查询是否存在预热值
                let warmUpKey = [code, DIDepth, lowWeight, highWeight, lowFund, highFund, j, relation].join('-');
                let warmUpValue = await cacheHandlers.getWarmUpPathsFromRedis(warmUpKey);
                let cacheKey = [j, code, relation, DIDepth, lowWeight, highWeight, lowFund, highFund, lowSubAmountRMB, highSubAmountRMB, isExtra, isBranches, surStatus, personalCode].join('-');
                if (!warmUpValue) {
                    // let nodeId = await findNodeId(code);
                    let queryBody = null;
                    let result = {};                                                                      //最终返回的结果
                    let directGuaranteePathQuery = null;
                    let directInvestPathQuery = null;

                    //for test
                    // surStatus = 0;

                    if (surStatus == 1 || surStatus == '1') {
                        if (isExtra == 0 || isExtra == '0') {
                            directGuaranteePathQuery = `match p= (from:company{ITCode2: ${code}})-[r:guarantees]->(to) where from.isExtra = '0' and to.isExtra = '0' return p`;
                            if (isBranches == 0 || isBranches == '0') {
                                directInvestPathQuery = `match p= (from:company{ITCode2: ${code}})-[r:invests*..${DIDepth}]->(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and rel.subAmountRMB >= ${lowSubAmountRMB} and rel.subAmountRMB <= ${highSubAmountRMB} and from.isExtra = '0' and to.isExtra = '0' and from.isBranches = '0' and to.isBranches = '0' and from.surStatus = '1' and to.surStatus = '1') return p`;
                            } else {
                                directInvestPathQuery = `match p= (from:company{ITCode2: ${code}})-[r:invests*..${DIDepth}]->(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and rel.subAmountRMB >= ${lowSubAmountRMB} and rel.subAmountRMB <= ${highSubAmountRMB} and from.isExtra = '0' and to.isExtra = '0' and from.surStatus = '1' and to.surStatus = '1') return p`;
                            }
                        } else {
                            directGuaranteePathQuery = `match p= (from:company{ITCode2: ${code}})-[r:guarantees]->(to) return p`;
                            if (isBranches == 0 || isBranches == '0') {
                                directInvestPathQuery = `match p= (from:company{ITCode2: ${code}})-[r:invests*..${DIDepth}]->(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and rel.subAmountRMB >= ${lowSubAmountRMB} and rel.subAmountRMB <= ${highSubAmountRMB} and from.isBranches = '0' and to.isBranches = '0' and from.surStatus = '1' and to.surStatus = '1') return p`;
                            } else {
                                directInvestPathQuery = `match p= (from:company{ITCode2: ${code}})-[r:invests*..${DIDepth}]->(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and rel.subAmountRMB >= ${lowSubAmountRMB} and rel.subAmountRMB <= ${highSubAmountRMB} and from.surStatus = '1' and to.surStatus = '1') return p`;
                            }
                        }
                    } else {
                        if (isExtra == 0 || isExtra == '0') {
                            directGuaranteePathQuery = `match p= (from:company{ITCode2: ${code}})-[r:guarantees]->(to) where from.isExtra = '0' and to.isExtra = '0' return p`;
                            if (isBranches == 0 || isBranches == '0') {
                                directInvestPathQuery = `match p= (from:company{ITCode2: ${code}})-[r:invests*..${DIDepth}]->(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and rel.subAmountRMB >= ${lowSubAmountRMB} and rel.subAmountRMB <= ${highSubAmountRMB} and from.isExtra = '0' and to.isExtra = '0' and from.isBranches = '0' and to.isBranches = '0') return p`;
                            } else {
                                directInvestPathQuery = `match p= (from:company{ITCode2: ${code}})-[r:invests*..${DIDepth}]->(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and rel.subAmountRMB >= ${lowSubAmountRMB} and rel.subAmountRMB <= ${highSubAmountRMB} and from.isExtra = '0' and to.isExtra = '0') return p`;
                            }
                        } else {
                            directGuaranteePathQuery = `match p= (from:company{ITCode2: ${code}})-[r:guarantees]->(to) return p`;
                            if (isBranches == 0 || isBranches == '0') {
                                directInvestPathQuery = `match p= (from:company{ITCode2: ${code}})-[r:invests*..${DIDepth}]->(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and rel.subAmountRMB >= ${lowSubAmountRMB} and rel.subAmountRMB <= ${highSubAmountRMB} and from.isBranches = '0' and to.isBranches = '0') return p`;
                            } else {
                                directInvestPathQuery = `match p= (from:company{ITCode2: ${code}})-[r:invests*..${DIDepth}]->(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and rel.subAmountRMB >= ${lowSubAmountRMB} and rel.subAmountRMB <= ${highSubAmountRMB}) return p`;
                            }
                        }
                    }

                    if (relation == 'invests') {
                        queryBody = directInvestPathQuery;
                    }
                    else if (relation == 'guarantees') {
                        queryBody = directGuaranteePathQuery;
                    }
                    if (!code) {
                        console.error(`${code} is not in the neo4j database at all !`);
                        logger.error(`${code} is not in the neo4j database at all !`);
                        return reject({ error: `${code}=nodeId` });
                    }
                    else if (code) {
                        let resultPromise = null;
                        let getCacheStart = Date.now();
                        //获取缓存
                        let previousValue = await cacheHandlers.getCache(cacheKey);
                        let getCacheCost = Date.now() - getCacheStart;
                        console.log('directInvestPath_getCacheCost: ' + getCacheCost + 'ms');
                        logger.info('directInvestPath_getCacheCost: ' + getCacheCost + 'ms');
                        if (!previousValue) {
                            let directInvestPathQueryStart = Date.now();
                            // resultPromise = await session.run(queryBody);

                            resultPromise = await sessionRun1(queryBody);
                            console.log('query neo4j server: ' +queryBody);
                            logger.info('query neo4j server: ' +queryBody);
                            let directInvestPathQueryCost = Date.now() - directInvestPathQueryStart;
                            logger.info(`${code} DirectInvestPathQueryCost: ` + directInvestPathQueryCost + 'ms');
                            console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + `, ${code} DirectInvestPathQueryCost: ` + directInvestPathQueryCost + 'ms');
                            if (resultPromise.records.length > 0) {
                                let result = await handlerNeo4jResult(resultPromise, lowFund, highFund);
                                let nodeResult = result.pathDetail.data.pathDetail;
                                if (nodeResult.length > 0) {
                                    let sortTreeResult = null;
                                    if (!personalCode) {
                                        sortTreeResult = pathTreeHandlers.fromTreePath1(nodeResult, code, relation);
                                    }
                                    else if (personalCode != null) {
                                        sortTreeResult = pathTreeHandlers.fromTreePath1(nodeResult, personalCode, relation);
                                    }
                                    cacheHandlers.setCache(cacheKey, JSON.stringify(sortTreeResult));
                                    //根据预热条件的阈值判断是否要加入预热
                                    let queryCostUp = config.warmUp_Condition.queryNeo4jCost;
                                    let recordsUp = config.warmUp_Condition.queryNeo4jRecords;
                                    if (directInvestPathQueryCost >= queryCostUp || resultPromise.records.length >= recordsUp) {
                                        let conditionsKey = config.redisKeyName.warmUpITCodes;
                                        let conditionsField = code;
                                        let conditionsValue = { ITCode: code, depth: [DIDepth], modes: [j], relations: [relation] };                                        //0代表对外投资，1代表股东
                                        cacheHandlers.setWarmUpConditionsToRedis(conditionsKey, conditionsField, JSON.stringify(conditionsValue));
                                        cacheHandlers.setWarmUpPathsToRedis(warmUpKey, JSON.stringify(sortTreeResult));
                                    }
                                    return resolve(sortTreeResult);
                                }
                                else if (nodeResult.length == 0) {
                                    let treeResult = { subLeaf: [], subLeafNum: 0 };
                                    cacheHandlers.setCache(cacheKey, JSON.stringify(treeResult));
                                    return resolve(treeResult);
                                }
                            }
                            else if (resultPromise.records.length == 0) {
                                let treeResult = { subLeaf: [], subLeafNum: 0 };;
                                cacheHandlers.setCache(cacheKey, JSON.stringify(treeResult));
                                return resolve(treeResult);
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
                console.error('queryDirectInvestPathError: ' + err);
                logger.error('queryDirectInvestPathError: ' + err);
                // driver.close();
                // session.close();
                return reject(err);
            }
        })
        // .timeout(lookupTimeout).catch( err => {
        //     console.log(err);
        //     logger.info(err);
        //     // return reject(err);
        // });                                                                                 //超时设置15s
    },

    //单个企业直接被投资关系路径查询
    queryDirectInvestedByPath: async function (code, relation, DIBDepth, lowWeight, highWeight, lowFund, highFund, lowSubAmountRMB, highSubAmountRMB, isExtra, isBranches) {
        return new Promise(async function (resolve, reject) {

            try {
                let j = 1;                                                                            //1代表DirectInvestedBy
                //先从redis中预热的数据中查询是否存在预热值
                let warmUpKey = [code, DIBDepth, lowWeight, highWeight, lowSubAmountRMB, highSubAmountRMB, j, relation].join('-');
                let warmUpValue = await cacheHandlers.getWarmUpPathsFromRedis(warmUpKey);
                let cacheKey = [j, code, relation, DIBDepth, lowWeight, highWeight, lowFund, highFund, lowSubAmountRMB, highSubAmountRMB, isExtra, isBranches].join('-');
                if (!warmUpValue) {
                    // let nodeId = await findNodeId(code);
                    let queryBody = null;
                    let directGuaranteedByPathQuery = null;
                    let directInvestedByPathQuery = null;

                    //for test
                    // surStatus = 0;

                    if (isExtra == 0 || isExtra == '0') {                                                              //过滤没有机构代码的nodes
                        // if (isBranches == 0 || isBranches == '0') {                                                    //过滤分支机构
                        //     directGuaranteedByPathQuery = `start from=node(${nodeId}) match p= (from)<-[r:guarantees}]-(to) where from.isExtra = '0' and to.isExtra = '0' and from.isBranches = '0' and to.isBranches = '0' return p`;
                        //     directInvestedByPathQuery = `start from=node(${nodeId}) match p= (from)<-[r:invests*..${DIBDepth}]-(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and rel.subAmountRMB >= ${lowSubAmountRMB} and rel.subAmountRMB <= ${highSubAmountRMB} and from.isExtra = '0' and to.isExtra = '0' and from.isBranches = '0' and to.isBranches = '0') return p`;
                        // } else {
                        //     directGuaranteedByPathQuery = `start from=node(${nodeId}) match p= (from)<-[r:guarantees]-(to) where from.isExtra = '0' and to.isExtra = '0' return p`;
                        //     directInvestedByPathQuery = `start from=node(${nodeId}) match p= (from)<-[r:invests*..${DIBDepth}]-(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and rel.subAmountRMB >= ${lowSubAmountRMB} and rel.subAmountRMB <= ${highSubAmountRMB} and from.isExtra = '0' and to.isExtra = '0') return p`;
                        // }

                        directGuaranteedByPathQuery = `match p= (from:company{ITCode2: ${code}})<-[r:guarantees]-(to) where from.isExtra = '0' and to.isExtra = '0' return p`;
                        directInvestedByPathQuery = `match p= (from:company{ITCode2: ${code}})<-[r:invests*..${DIBDepth}]-(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and rel.subAmountRMB >= ${lowSubAmountRMB} and rel.subAmountRMB <= ${highSubAmountRMB} and from.isExtra = '0' and to.isExtra = '0') return p`;
                    } else {
                        // if (isBranches == 0 || isBranches == '0') {
                        //     directGuaranteedByPathQuery = `start from=node(${nodeId}) match p= (from)<-[r:guarantees]-(to) where from.isBranches = '0' and to.isBranches = '0' return p`;
                        //     directInvestedByPathQuery = `start from=node(${nodeId}) match p= (from)<-[r:invests*..${DIBDepth}]-(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and rel.subAmountRMB >= ${lowSubAmountRMB} and rel.subAmountRMB <= ${highSubAmountRMB} and from.isBranches = '0' and to.isBranches = '0') return p`;
                        // } else {
                        //     directGuaranteedByPathQuery = `start from=node(${nodeId}) match p= (from)<-[r:guarantees]-(to) return p`;
                        //     directInvestedByPathQuery = `start from=node(${nodeId}) match p= (from)<-[r:invests*..${DIBDepth}]-(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and rel.subAmountRMB >= ${lowSubAmountRMB} and rel.subAmountRMB <= ${highSubAmountRMB}) return p`;
                        // }
                        directGuaranteedByPathQuery = `match p= (from:company{ITCode2: ${code}})<-[r:guarantees]-(to) return p`;
                        directInvestedByPathQuery = `match p= (from:company{ITCode2: ${code}})<-[r:invests*..${DIBDepth}]-(to) where all(rel in r where rel.weight >= ${lowWeight} and rel.weight <= ${highWeight} and rel.subAmountRMB >= ${lowSubAmountRMB} and rel.subAmountRMB <= ${highSubAmountRMB}) return p`;
                    }

                    if (relation == 'invests') {
                        queryBody = directInvestedByPathQuery;
                    }
                    else if (relation == 'guarantees') {
                        queryBody = directGuaranteedByPathQuery;
                    }
                    if (!code) {
                        console.error(`${code} is not in the neo4j database at all !`);
                        logger.error(`${code} is not in the neo4j database at all !`);
                        return reject({ error: `${code}=nodeId` });
                    }
                    else if (code) {
                        let resultPromise = null;
                        let getCacheStart = Date.now();
                        //获取缓存
                        let previousValue = await cacheHandlers.getCache(cacheKey);
                        let getCacheCost = Date.now() - getCacheStart;
                        console.log('directInvestedByPath_getCacheCost: ' + getCacheCost + 'ms');
                        logger.info('directInvestedByPath_getCacheCost: ' + getCacheCost + 'ms');
                        if (!previousValue) {
                            let directInvestedByPathQueryStart = Date.now();
                            // resultPromise = await session.run(queryBody);

                            resultPromise = await sessionRun2(queryBody);
                            console.log('query neo4j server: ' +queryBody);
                            logger.info('query neo4j server: ' +queryBody);
                            let directInvestedByPathQueryCost = Date.now() - directInvestedByPathQueryStart;
                            logger.info(`${code} directInvestedByPathQueryCost: ` + directInvestedByPathQueryCost + 'ms');
                            console.log("Time: " + moment(Date.now()).format("YYYY-MM-DD HH:mm:ss") + `, ${code} directInvestedByPathQueryCost: ` + directInvestedByPathQueryCost + 'ms');
                            if (resultPromise.records.length > 0) {
                                let result = await handlerNeo4jResult(resultPromise, lowFund, highFund);
                                let nodeResult = result.pathDetail.data.pathDetail;
                                if (nodeResult.length > 0) {
                                    let sortTreeResult = pathTreeHandlers.fromTreePath2(nodeResult, code, relation);
                                    cacheHandlers.setCache(cacheKey, JSON.stringify(sortTreeResult));
                                    //根据预热条件的阈值判断是否要加入预热
                                    let queryCostUp = config.warmUp_Condition.queryNeo4jCost;
                                    let recordsUp = config.warmUp_Condition.queryNeo4jRecords;
                                    if (directInvestedByPathQueryCost >= queryCostUp || resultPromise.records.length >= recordsUp) {
                                        let conditionsKey = config.redisKeyName.warmUpITCodes;
                                        let conditionsField = code;
                                        let conditionsValue = { ITCode: code, depth: [DIBDepth], modes: [j], relations: [relation] };                                        //0代表对外投资，1代表股东
                                        cacheHandlers.setWarmUpConditionsToRedis(conditionsKey, conditionsField, JSON.stringify(conditionsValue));
                                        cacheHandlers.setWarmUpPathsToRedis(warmUpKey, JSON.stringify(sortTreeResult));
                                    }
                                    return resolve(sortTreeResult);
                                }
                                else if (nodeResult.length == 0) {
                                    let treeResult = { subLeaf: [], subLeafNum: 0 };
                                    cacheHandlers.setCache(cacheKey, JSON.stringify(treeResult));
                                    return resolve(treeResult);
                                }
                            }
                            else if (resultPromise.records.length == 0) {
                                let treeResult = { subLeaf: [], subLeafNum: 0 };
                                cacheHandlers.setCache(cacheKey, JSON.stringify(treeResult));
                                return resolve(treeResult);
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
                console.error('queryDirectInvestedByPathError: ' + err);
                logger.error('queryDirectInvestedByPathError: ' + err);
                // driver.close();
                // session.close();
                return reject(err);
            }
        })
        // .timeout(lookupTimeout).catch( err => {
        //     console.log(err);
        //     logger.info(err);
        //     // return reject(err);
        // });                                                                  //超时设置15s
    }

}

module.exports = searchGraph;