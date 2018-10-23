/*
用于处理arangodb返回的数据
wrote by tzf, 2018/10/17
*/
const cacheHandlers = require('./cacheHandlers.js');
const req = require('require-yml')
const config = req("config/source.yml");
const log4js = require('log4js');
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

//处理ITCode2查询ITName为空的情况
function handlerAllNames(names) {
    let newNames = [];
    for (let subName of names) {
        if (subName != '') {
            newNames.push(subName);
        }
        else if (subName == '') {
            subName = 'ITName is null!';
            newNames.push(subName);
        }
    }
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

//根据key筛选node信息
function filterNode(nodes, key) {
    let result = {
        name: null,
        isPerson: 0,
        RMBFund: 0,
        regFund: 0,
        regFundUnit: '万元人民币',
        isExtra: 0,
        surStatus: 1,
        isBranches: 0
    };
    for (let i = 0; i < nodes.length; i++) {
        if (nodes[i]._key == key) {
            result.ITCode2 = nodes[i]._key;
            if (nodes[i].hasOwnProperty('name')) {
                result.name = nodes[i].name;
            }
            if (nodes[i].hasOwnProperty('isPerson')) {
                result.isPerson = nodes[i].isPerson;
            }
            if (nodes[i].hasOwnProperty('RMBFund')) {
                result.RMBFund = nodes[i].RMBFund;
            }
            if (nodes[i].hasOwnProperty('regFund')) {
                result.regFund = nodes[i].regFund;
            }
            if (nodes[i].hasOwnProperty('regFundUnit')) {
                result.regFundUnit = nodes[i].regFundUnit;
            }
            if (nodes[i].hasOwnProperty('isExtra')) {
                result.isExtra = nodes[i].isExtra;
            }
            if (nodes[i].hasOwnProperty('surStatus')) {
                result.surStatus = nodes[i].surStatus;
            }
            if (nodes[i].hasOwnProperty('isBranches')) {
                result.isBranches = nodes[i].isBranches;
            }
            result.filterId = i + 1;
            break;
        }
    }
    // result.leftNodes = nodes.splice(result.filterId, 1);
    return result;
}

//获取edge信息
function filterEdge(edge) {
    let result = {
        shareholdRatio: 0,
        shareholdQuantityRMB: 0,
        shareholdQuantity: 0,
        shareholdQuantityUnit: '万人民币元',
        relation: null,
        relationCode: 0,
        relationName: null
    }
    if (edge.hasOwnProperty('weight')) {
        result.shareholdRatio = edge.weight;
    }
    if (edge.hasOwnProperty('subAmountRMB')) {
        result.shareholdQuantityRMB = edge.subAmountRMB;
    }
    if (edge.hasOwnProperty('subAmount')) {
        result.shareholdQuantity = edge.subAmount;
    }
    if (edge.hasOwnProperty('subAmountUnit')) {
        result.shareholdQuantityUnit = edge.subAmountUnit;
    }
    if (edge.hasOwnProperty('relation')) {
        result.relation = edge.relation;
    }
    if (edge.hasOwnProperty('relationCode')) {
        result.relationCode = edge.relationCode;
    }
    if (edge.hasOwnProperty('relationName')) {
        result.relationName = edge.relationName;
    }
    return result;
}

//判断重复的path
function isDuplicatedPath(pathGroup, pathDetail) {
    let flag = false;
    let groupLen = pathGroup.length;
    let breakTimes = 0;
    if (groupLen != 0) {
        for (let i = 0; i < groupLen; i++) {
            let groupPathLen = pathGroup[i].path.length;
            let pathDetailLen = pathDetail.path.length;
            if (groupPathLen == pathDetailLen) {
                for (let j = 0; j < groupPathLen; j++) {
                    let path_one = pathGroup[i].path[j];
                    let path_two = pathDetail.path[j];
                    if (path_one.sourceCode != path_two.sourceCode || path_one.targetCode != path_two.targetCode) {
                        breakTimes++;
                        break;
                    }
                }
            }
            else {
                breakTimes++;
            }
        }
        console.log('groupLen: ' + groupLen + ', breakTimes: ' + breakTimes + '次');
        if (breakTimes != groupLen) {
            flag = true;                                                //eachPath的sourceCode 和 targetCode与pathArray中path有一致的存在，即重复的path 
            console.log('过滤1条重复path！');
        }
    }
    return flag;
}

//判断每个path下是否存在source/target出现2次以上的情况(source >=2: 共同被投资; target >=2: 共同被投资)
function findCodeAppearTimes(pathDetail) {
    let sourceArray = [];
    let targetArray = [];
    let result = { sourceIsRepeat: false, targetIsRepeat: false };
    for (let subPathDetail of pathDetail) {
        let sourceCode = subPathDetail.sourceCode;
        let targetCode = subPathDetail.targetCode;
        sourceArray.push(sourceCode);
        targetArray.push(targetCode);
    }
    result.sourceIsRepeat = isRepeat(sourceArray);
    result.targetIsRepeat = isRepeat(targetArray);
    return result;
}

//数组是否存在重复元素
function isRepeat(array) {
    let hash = {};
    for (let i in array) {
        if (hash[array[i]]) {
            return true;
        }
        hash[array[i]] = true;
    }
    return false;
}


//判断invest path的source/target连接是否有断点, 即是否有guarantees关系连接才形成的path
function isContinuousPath(from, to, pathArray) {
    let result = { flag: true, pathDetail: [] };
    let isFromToDirection = 1;
    let pathLength = pathArray.length;
    let newPathArray = [];
    if (pathLength > 1) {
        if ((pathArray[0].targetCode == to && pathArray[0].sourceCode != from) || (pathArray[0].targetCode == from && pathArray[0].sourceCode != to)) {
            isFromToDirection = 0
        }
        //如果from<-to方向的path需倒序排列
        if (isFromToDirection == 0) {
            for (let i = pathLength - 1; i >= 0; i--) {
                newPathArray.push(pathArray[i]);
            }
            pathArray = newPathArray;
        }
        for (let i = 0; i < pathLength - 1; i++) {
            let nextSourceCode = pathArray[i + 1].sourceCode;
            let targetCode = pathArray[i].targetCode;
            let sourceCode = pathArray[i].sourceCode;
            let nextTargetCode = pathArray[i + 1].targetCode;
            if (targetCode != nextSourceCode || sourceCode == nextTargetCode || targetCode == from || targetCode == to) {                     //targetCode != nextSourceCode:不连续的path;  sourceCode == nextTargetCode：闭环的path
                console.log('该invest path不符合要求！');
                result.flag = false;
                break;
            }
        }
    }
    result.pathDetail = pathArray;
    return result;
}

//判断invest path的是否闭环
function isClosedLoopPath(from, to, pathArray) {
    // from = parseInt(from);
    // to = parseInt(to);
    let flag = false;
    let pathLength = pathArray.length;
    if (pathLength > 1) {
        for (let i = 0; i < pathLength; i++) {
            let sourceCode = pathArray[i].sourceCode;
            let targetCode = pathArray[i].targetCode;
            if ((sourceCode == from && targetCode == to) || (sourceCode == to && targetCode == from)) {
                console.log('该invest path不符合要求！');
                flag = true;
                break;
            }
        }
    }
    return flag;
}

//从path找出personalCode
function findPersonalCode(path) {
    let perCode = 0;
    if (path.length > 0) {
        for (let i = 0; i < path.length; i++) {
            let sourceIsPerson = path[i].sourceIsPerson;
            if (sourceIsPerson == 1 || sourceIsPerson == '1') {
                perCode = path[i].sourceCode;
                break;
            }
            else {
                let targetIsPerson = path[i].targetIsPerson;
                if (targetIsPerson == 1 || targetIsPerson == '1') {
                    perCode = path[i].targetCode;
                    break;
                }
            }
        }
    }
    return perCode;
}

let resultHandlers = {
    //处理arangodb返回的JSON数据格式
    handlerPromise1: async function (result, index) {
        let allNames = [];
        let allCodes = new Set();                                        //使用set保证数据的唯一性
        let uniqueCodes = [];                                            //存储唯一性的codes数据元素
        let num = 0;
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

        promiseResult.dataDetail.data.pathNum = result.length;
        num += result.length;                                           //返回的所有路径，包括有重复点的路径

        if (result.length == 0)
            return promiseResult;
        else {
            let paths = [];
            for (let subRecord of result) {
                let pathArray = {};
                let tempPathArray = [];
                let nodes = subRecord.vertices;
                for (let edge of subRecord.edges) {
                    let startEdgeCode = edge._from.replace(/nodes\//g, '');
                    let endEdgeCode = edge._to.replace(/nodes\//g, '');
                    let startInfo = filterNode(nodes, startEdgeCode);
                    let endInfo = filterNode(nodes, endEdgeCode);

                    //处理无机构代码的start nodes的name问题
                    let startName = null;
                    //取到start nodes的isExtra属性
                    let startIsExtra = startInfo.isExtra;
                    if (startIsExtra == 1 || startIsExtra == '1' || startIsExtra == 'null') {
                        startName = startInfo.name;
                    }
                    else if (startIsExtra == 0 || startIsExtra == '0') {
                        allCodes.add(startEdgeCode);                                                                     //将有机构代码的ITCode存入数组
                    }

                    //处理无机构代码的end nodes的name问题
                    let endName = null;
                    //取到end nodes的isExtra属性
                    let endIsExtra = endInfo.isExtra;
                    if (endIsExtra == 1 || endIsExtra == '1' || endIsExtra == 'null') {
                        endName = endInfo.name;
                    }
                    else if (endIsExtra == 0 || endIsExtra == '0') {
                        allCodes.add(endEdgeCode);                                                                     //将有机构代码的ITCode存入数组
                    }

                    let edgeInfo = filterEdge(edge);

                    let path = {
                        sourceCode: startEdgeCode,
                        source: startName,
                        sourceRegCapitalRMB: startInfo.RMBFund,
                        sourceRegCapital: startInfo.regFund,
                        sourceRegCapitalUnit: startInfo.regFundUnit,
                        sourceIsExtra: startInfo.isExtra,
                        sourceIsPerson: startInfo.isPerson,
                        shareholdRatio: edgeInfo.shareholdRatio,
                        shareholdQuantityRMB: edgeInfo.shareholdQuantityRMB,
                        shareholdQuantity: edgeInfo.shareholdQuantity,
                        shareholdQuantityUnit: edgeInfo.shareholdQuantityUnit,
                        targetCode: endEdgeCode,
                        target: endName,
                        targetRegCapitalRMB: endInfo.RMBFund,
                        targetRegCapital: endInfo.regFund,
                        targetRegCapitalUnit: endInfo.regFundUnit,
                        targetIsExtra: endInfo.isExtra,
                        targetIsPerson: endInfo.isPerson
                    };

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
                }
                paths.push(pathArray);
            }
            if (Object.keys(paths).length != 0) {
                promiseResult.dataDetail.data.pathDetail = paths;
            }
            promiseResult.dataDetail.data.pathNum = num;
        }
        uniqueCodes = Array.from(allCodes);
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
    },

    handlerPromise2: async function (from, to, result, index) {
        //判断from、to是否自然人的personalCode
        let fromIsPerson = 0;
        let toIsPerson = 0;
        if (null != from && from.indexOf('P') >= 0) {
            fromIsPerson = 1;
        }
        if (null != to && to.indexOf('P') >= 0) {
            toIsPerson = 1;
        }

        let allNamesOne = [];
        let allCodesOne = new Set();                                       //使用set保证数据的唯一性
        let newAllCodesOne = new Set();                                       //含自然人
        let newCodesOne = new Set();                                       //不含自然人
        let uniqueCodesOne = [];                                           //存储唯一性的codes数据元素
        let allNamesTwo = [];
        let allCodesTwo = new Set();                                       //使用set保证数据的唯一性
        let uniqueCodesTwo = [];                                           //存储唯一性的codes数据元素

        let uniqueCodesThree = new Set();
        let uniqueNamesThree = new Set();
        let uniqueCodesThreeArray = [];
        let uniqueNamesThreeArray = [];

        let allNamesFour = [];
        let uniqueCodesFour = new Set();
        let uniqueNamesFour = new Set();
        let uniqueCodesFourArray = [];
        let uniqueNamesFourArray = [];

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
        else if (index == 8)
            pathTypeName = "GuaranteePath";
        else if (index == 9)
            pathTypeName = "GuaranteedByPath";
        let promiseResult = { pathTypeOne: {}, pathTypeTwo: {} };
        let promiseResultOne = { toPaNum: 0, dataDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_${index}`, typeName: pathTypeName, names: [] } };
        let promiseResultTwo = { toPaNum: 0, dataDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_10`, typeName: 'guarantees', names: [], pathTypeName } };
        let promiseResultThree = { toPaNum: 0, dataDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_11`, typeName: 'family', names: [] } };
        let promiseResultFour = { toPaNum: 0, dataDetail: { data: { pathDetail: [], pathNum: 0 }, type: `result_12`, typeName: 'executiveInvestPath', names: [] } };

        if (result.length == 0) {
            promiseResult.pathTypeOne = promiseResultOne;
            promiseResult.pathTypeTwo = promiseResultTwo;
            promiseResult.pathTypeThree = promiseResultThree;
            promiseResult.pathTypeFour = promiseResultFour;
            return promiseResult;
        }
        else if (result.length > 0) {
            let tempResultTwo = [];                                                                          //临时存放所有的担保关系路径
            let newTempResultTwo = [];                                                                       //筛选后的担保关系路径
            let tempResultThree = [];                                                                        //存放家族关系路径
            let newTempResultThree = [];

            for (let subRecord of result) {
                let pathOneCodeSet = new Set();                                                              //存放pathOne中所有的ITCode2(除自然人)
                let pathArrayOne = {};
                let pathArrayTwo = {};
                let pathArrayThree = {};
                let pathArrayFour = {};
                let tempPathArrayOne = [];
                let tempPathArrayTwo = [];
                let tempPathArrayThree = [];
                let tempPathArrayFour = [];
                let nodes = subRecord.vertices;
                for (let edge of subRecord.edges) {
                    let startEdgeCode = edge._from.replace(/nodes\//g, '');
                    let endEdgeCode = edge._to.replace(/nodes\//g, '');
                    let startInfo = filterNode(nodes, startEdgeCode);
                    let endInfo = filterNode(nodes, endEdgeCode);
                    //取到start nodes的isPerson属性
                    let startIsPerson = startInfo.isPerson;
                    //取到end nodes的isPerson属性
                    let endIsPerson = endInfo.isPerson;
                    //取到start node的fund(注册资本)属性
                    let startRegFund = startInfo.RMBFund;
                    let edgeInfo = filterEdge(edge);

                    //获取每个path relationship type
                    let relationshipType = edgeInfo.relation;
                    //获取edge上的属性
                    let weight = edgeInfo.shareholdRatio;                                                                    //持股比例
                    let relCode = edgeInfo.relationCode;                                                                   //家族关系代码
                    let relName = edgeInfo.relationName;                                                                //家族关系名称

                    let endRegFund = endInfo.RMBFund;                                                               //注册资金

                    //处理无机构代码的start nodes的name问题
                    let startName = null;
                    //取到start nodes的isExtra属性
                    let startIsExtra = startInfo.isExtra;
                    if (startIsExtra == 1 || startIsExtra == '1' || startIsExtra == 'null') {
                        startName = startInfo.name;
                    }

                    //处理无机构代码的end nodes的name问题
                    let endName = null;
                    //取到end nodes的isExtra属性
                    let endIsExtra = endInfo.isExtra;
                    if (endIsExtra == 1 || endIsExtra == '1' || endIsExtra == 'null') {
                        endName = endInfo.name;
                    }

                    let pathOne = {                                                                               //invests的path
                        source: null, sourceCode: null, sourceRegFund: null, sourceIsExtra: null, sourceIsPerson: null,
                        target: null, targetCode: null, targetRegFund: null, targetIsExtra: null, targetIsPerson: null, weight: null
                    };
                    let pathTwo = {                                                                               //guarantees的path
                        source: null, sourceCode: null, sourceRegFund: null, sourceIsExtra: null, sourceIsPerson: null,
                        target: null, targetCode: null, targetRegFund: null, targetIsExtra: null, targetIsPerson: null, weight: null
                    };
                    let pathThree = {                                                                             //family的path
                        source: null, sourceCode: null, sourceIsExtra: null, sourceIsPerson: null,
                        target: null, targetCode: null, targetIsExtra: null, targetIsPerson: null, relationCode: null, relationName: null
                    };
                    let pathFour = {                                                                             //executes的path
                        compCode: null, compName: null, RMBRegFund: null, isExtra: null, isPerson: null
                    };

                    if (relationshipType == 'invests' || !relationshipType) {
                        pathOne.source = startName;
                        pathOne.sourceCode = startEdgeCode;
                        pathOne.sourceRegFund = startRegFund;
                        pathOne.sourceIsExtra = startIsExtra;
                        pathOne.sourceIsPerson = startIsPerson;
                        pathOne.target = endName;
                        pathOne.targetCode = endEdgeCode;
                        pathOne.targetRegFund = endRegFund;
                        pathOne.targetIsExtra = endIsExtra;
                        pathOne.targetIsPerson = endIsPerson;
                        pathOne.weight = weight;
                        // pathOne.pathType = relationshipType;

                        //如果isPerson = true 过滤掉
                        if (startIsPerson != 1 || startIsPerson != '1') {
                            pathOneCodeSet.add(startEdgeCode);
                        }
                        if (endIsPerson != 1 || endIsPerson != '1') {
                            pathOneCodeSet.add(endEdgeCode);
                        }


                    }
                    else if (relationshipType == 'guarantees') {
                        pathTwo.source = startName;
                        pathTwo.sourceCode = startEdgeCode;
                        pathTwo.sourceRegFund = startRegFund;
                        pathTwo.sourceIsExtra = startIsExtra;
                        pathTwo.sourceIsPerson = startIsPerson;
                        pathTwo.target = endName;
                        pathTwo.targetCode = endEdgeCode;
                        pathTwo.targetRegFund = endRegFund;
                        pathTwo.targetIsExtra = endIsExtra;
                        pathTwo.targetIsPerson = endIsPerson;
                        pathTwo.weight = weight;
                        // pathTwo.pathType = relationshipType;

                    }
                    else if (relationshipType == 'family') {
                        pathThree.source = startName;
                        pathThree.sourceCode = startEdgeCode;
                        pathThree.sourceIsExtra = startIsExtra;
                        pathThree.sourceIsPerson = startIsPerson;
                        pathThree.target = endName;
                        pathThree.targetCode = endEdgeCode;
                        pathThree.targetIsExtra = endIsExtra;
                        pathThree.targetIsPerson = endIsPerson;
                        pathThree.relationCode = relCode;
                        pathThree.relationName = relName;

                    }
                    else if (relationshipType == 'executes') {
                        pathFour.compCode = endEdgeCode;
                        if (endIsExtra == 1 || endIsExtra == '1') {
                            pathFour.compName = endName;
                            uniqueNamesFour.add(endName);
                        }
                        else {
                            uniqueCodesFour.add(endEdgeCode);
                        }
                        pathFour.RMBRegFund = endRegFund;
                        pathFour.isExtra = endIsExtra;
                        pathFour.isPerson = endIsPerson;

                    }

                    if (null != pathOne.sourceCode && null != pathOne.targetCode) {
                        // delete pathOne.pathType;                                             //删除pathType属性
                        tempPathArrayOne.push(pathOne);
                    }
                    //invests关系存在的前提下, 添加guarantees关系的path
                    if (null != pathTwo.sourceCode && null != pathTwo.targetCode && pathTwo.sourceCode != pathTwo.targetCode) {
                        // delete pathTwo.pathType;
                        tempPathArrayTwo.push(pathTwo);
                    }
                    if (null != pathThree.sourceCode && null != pathThree.targetCode) {
                        tempPathArrayThree.push(pathThree);                                        //存放家族关系路径
                    }
                    if (null != pathFour.compCode) {
                        tempPathArrayFour.push(pathFour);
                    }
                }

                //处理投资关系--pathArrayOne
                if (null != from && null != to && tempPathArrayOne.length > 0) {
                    //判断form/to中是否有自然人
                    if (fromIsPerson == 1 || toIsPerson == 1) {
                        pathArrayOne.path = tempPathArrayOne;
                        if (fromIsPerson == 1 && toIsPerson == 0) {
                            let sourceCodeOne = findPersonalCode(tempPathArrayOne);
                            if (sourceCodeOne == from) {
                                pathArrayOne.isMainPath = 1;
                            }
                            else {
                                pathArrayOne.isMainPath = 0;
                            }
                        }
                        else if (toIsPerson == 1 && fromIsPerson == 0) {
                            let sourceCodeOne = findPersonalCode(tempPathArrayOne);
                            if (sourceCodeOne == to) {
                                pathArrayOne.isMainPath = 1;
                            }
                            else {
                                pathArrayOne.isMainPath = 0;
                            }
                        }
                        else {
                            pathArrayOne.isMainPath = 1;
                        }
                    }
                    else if (fromIsPerson == 0 && toIsPerson == 0) {
                        //如果from和to任何一个不在pathOne中，则剔除这条path
                        if (pathOneCodeSet.has(from) && pathOneCodeSet.has(to)) {
                            pathArrayOne.path = tempPathArrayOne;
                            pathArrayOne.isMainPath = 1;
                            //将每个path下的pathOneCodeSet中的元素添加到allCodesOne中
                            allCodesOne = new Set([...allCodesOne, ...pathOneCodeSet]);
                        }
                    }
                    //处理投资关系--pathArrayOne
                    if (pathArrayOne.hasOwnProperty('path') && pathArrayOne.path.length > 0) {
                        // promiseResultOne.dataDetail.data.pathDetail.push(pathArrayOne);
                        //过滤重复的path
                        let pathGroup = promiseResultOne.dataDetail.data.pathDetail;
                        let isDupFlag = isDuplicatedPath(pathGroup, pathArrayOne);
                        if (isDupFlag == false) {

                            //判断form/to都不是自然人
                            // if (from.indexOf('P') < 0 && to.indexOf('P') < 0) {
                            //处理共同投资和共同股东关系路径
                            if (index == 4 || index == 5) {
                                let sourceCodeVal = pathArrayOne.path[0].sourceCode;
                                let targetCodeVal = pathArrayOne.path[0].targetCode;
                                let fromCode = from;
                                let toCode = to;
                                let result = findCodeAppearTimes(pathArrayOne.path);
                                //如果source/target都出现2次以上，过滤掉
                                if (result.sourceIsRepeat == true && result.targetIsRepeat == true) {
                                    console.log('过滤1条出现重复节点的path!');
                                }
                                else if (result.sourceIsRepeat == false || result.targetIsRepeat == false) {
                                    //判断path是否闭环
                                    let isClosedLoopFlag = isClosedLoopPath(from, to, pathArrayOne.path);
                                    if (isClosedLoopFlag == false && pathArrayOne.path.length > 1 && (result.sourceIsRepeat == true || result.targetIsRepeat == true)) {
                                        //处理共同投资关系路径
                                        if (index == 4 && result.targetIsRepeat == true) {
                                            if ((sourceCodeVal == fromCode && targetCodeVal == toCode) || (sourceCodeVal == toCode && targetCodeVal == fromCode)) {
                                                console.log('delete the path !');
                                            }
                                            else {
                                                promiseResultOne.dataDetail.data.pathDetail.push(pathArrayOne);
                                                for (let subPath of pathArrayOne.path) {
                                                    let isPerson_source = subPath.sourceIsPerson;
                                                    let isPerson_target = subPath.targetIsPerson;
                                                    let ITCode_source = subPath.sourceCode;
                                                    let ITCode_target = subPath.targetCode;
                                                    if (isPerson_source != 1 && isPerson_source != '1') {
                                                        newCodesOne.add(`${ITCode_source}`);
                                                    }
                                                    if (isPerson_target != 1 && isPerson_target != '1') {
                                                        newCodesOne.add(`${ITCode_target}`);
                                                    }
                                                    newAllCodesOne.add(ITCode_source);
                                                    newAllCodesOne.add(ITCode_target);
                                                }
                                            }
                                        }
                                        //处理共同股东关系路径
                                        if (index == 5 && result.sourceIsRepeat == true) {
                                            if ((sourceCodeVal == fromCode && targetCodeVal == toCode) || (sourceCodeVal == toCode && targetCodeVal == fromCode)) {
                                                console.log('delete the path !');
                                            }
                                            else {
                                                promiseResultOne.dataDetail.data.pathDetail.push(pathArrayOne);
                                                for (let subPath of pathArrayOne.path) {
                                                    let isPerson_source = subPath.sourceIsPerson;
                                                    let isPerson_target = subPath.targetIsPerson;
                                                    let ITCode_source = subPath.sourceCode;
                                                    let ITCode_target = subPath.targetCode;
                                                    if (isPerson_source != 1 && isPerson_source != '1') {
                                                        newCodesOne.add(`${ITCode_source}`);
                                                    }
                                                    if (isPerson_target != 1 && isPerson_target != '1') {
                                                        newCodesOne.add(`${ITCode_target}`);
                                                    }
                                                    newAllCodesOne.add(ITCode_source);
                                                    newAllCodesOne.add(ITCode_target);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            //处理直接投资关系路径
                            else if (index == 0) {
                                let flag = false;
                                let result = isContinuousPath(from, to, pathArrayOne.path);
                                flag = result.flag;
                                pathArrayOne.path = result.pathDetail;
                                if (flag == true) {
                                    promiseResultOne.dataDetail.data.pathDetail.push(pathArrayOne);
                                    for (let subPath of pathArrayOne.path) {
                                        let isPerson_source = subPath.sourceIsPerson;
                                        let isPerson_target = subPath.targetIsPerson;
                                        let ITCode_source = subPath.sourceCode;
                                        let ITCode_target = subPath.targetCode;
                                        if (isPerson_source != 1 && isPerson_source != '1') {
                                            newCodesOne.add(`${ITCode_source}`);
                                        }
                                        if (isPerson_target != 1 && isPerson_target != '1') {
                                            newCodesOne.add(`${ITCode_target}`);
                                        }
                                        newAllCodesOne.add(ITCode_source);
                                        newAllCodesOne.add(ITCode_target);
                                    }
                                }
                            }
                            //处理直接被投资关系路径
                            else if (index == 1) {
                                let flag = false;
                                let result = isContinuousPath(from, to, pathArrayOne.path);
                                flag = result.flag;
                                pathArrayOne.path = result.pathDetail;
                                if (flag == true) {
                                    promiseResultOne.dataDetail.data.pathDetail.push(pathArrayOne);
                                    for (let subPath of pathArrayOne.path) {
                                        let isPerson_source = subPath.sourceIsPerson; 7
                                        let isPerson_target = subPath.targetIsPerson;
                                        let ITCode_source = subPath.sourceCode;
                                        let ITCode_target = subPath.targetCode;
                                        if (isPerson_source != 1 && isPerson_source != '1') {
                                            newCodesOne.add(`${ITCode_source}`);
                                        }
                                        if (isPerson_target != 1 && isPerson_target != '1') {
                                            newCodesOne.add(`${ITCode_target}`);
                                        }
                                        newAllCodesOne.add(ITCode_source);
                                        newAllCodesOne.add(ITCode_target);
                                    }
                                }
                            }
                            //处理最短路径等其他关系路径
                            else {
                                promiseResultOne.dataDetail.data.pathDetail.push(pathArrayOne);
                                for (let subPath of pathArrayOne.path) {
                                    let isPerson_source = subPath.sourceIsPerson;
                                    let isPerson_target = subPath.targetIsPerson;
                                    let ITCode_source = subPath.sourceCode;
                                    let ITCode_target = subPath.targetCode;
                                    if (isPerson_source != 1 && isPerson_source != '1') {
                                        newCodesOne.add(`${ITCode_source}`);
                                    }
                                    if (isPerson_target != 1 && isPerson_target != '1') {
                                        newCodesOne.add(`${ITCode_target}`);
                                    }
                                    newAllCodesOne.add(ITCode_source);
                                    newAllCodesOne.add(ITCode_target);
                                }
                            }
                        }
                    }
                }

                pathArrayTwo.path = tempPathArrayTwo;
                if (tempPathArrayThree.length > 0) {
                    pathArrayThree.path = [tempPathArrayThree[0]];                                  //family关系只取一层关系
                    // pathArrayThree.path = tempPathArrayThree;
                }

                //处理担保关系--pathArrayTwo
                if (pathArrayTwo.hasOwnProperty('path') && pathArrayTwo.path.length > 0) {
                    tempResultTwo.push(pathArrayTwo);
                }

                //处理家族关系--pathArrayThree
                if (pathArrayThree.hasOwnProperty('path') && pathArrayThree.path.length > 0) {
                    //过滤重复path
                    let pathGroup = [];
                    if (tempResultThree.length > 0) {
                        for (let subResultThree of tempResultThree) {
                            pathGroup.push(subResultThree);
                        }
                    }
                    if (pathGroup.length > 0) {
                        let isDupFlag = isDuplicatedPath(pathGroup, pathArrayThree);
                        if (isDupFlag == false) {
                            tempResultThree.push(pathArrayThree);
                        }
                    }
                    else if (pathGroup.length == 0) {
                        tempResultThree.push(pathArrayThree);
                    }
                }

                //处理高管投资关系--pathArrayFour
                pathArrayFour.path = tempPathArrayFour;
                promiseResultFour.dataDetail.data.pathDetail.push(pathArrayFour);

            }

            uniqueCodesOne = Array.from(newCodesOne);
            // uniqueCodesThreeArray = Array.from(uniqueCodesThree);
            // uniqueNamesThreeArray = Array.from(uniqueNamesThree);
            uniqueCodesFourArray = Array.from(uniqueCodesFour);
            uniqueNamesFourArray = Array.from(uniqueNamesFour);

            //根据投资关系pathArrayOne中涉及到的机构才展示担保关系pathArrayTwo
            for (let subResult of tempResultTwo) {
                let newSubResult = { path: [] };
                for (let subPathDetail of subResult.path) {
                    let sourceCode_two = subPathDetail.sourceCode;
                    let targetCode_two = subPathDetail.targetCode;
                    if (newCodesOne.has(`${sourceCode_two}`) == true && newCodesOne.has(`${targetCode_two}`) == true || index == 8 || index == 9) {
                        allCodesTwo.add(`${sourceCode_two}`);
                        allCodesTwo.add(`${targetCode_two}`);
                        newSubResult.path.push(subPathDetail);
                    }
                }
                if (newSubResult.path.length > 0) {
                    newTempResultTwo.push(newSubResult);
                    //过滤重复的path
                    let pathGroup = promiseResultTwo.dataDetail.data.pathDetail;
                    let isDupFlag = isDuplicatedPath(pathGroup, newSubResult);
                    if (isDupFlag == false) {
                        promiseResultTwo.dataDetail.data.pathDetail.push(newSubResult);
                    }
                }
            }
            uniqueCodesTwo = Array.from(allCodesTwo);

            //根据投资关系pathArrayOne中涉及到的机构才展示家族关系pathArrayThree
            for (let subResult of tempResultThree) {
                let newSubResult = { path: [] };
                for (let subPathDetail of subResult.path) {
                    let sourceCode_three = subPathDetail.sourceCode;
                    let targetCode_three = subPathDetail.targetCode;
                    //直接投资关系(被投资关系)时，from/to有自然人，不过滤
                    if ((index == 0 || index == 1) && (fromIsPerson == 1 || toIsPerson == 1)) {
                        newSubResult.path.push(subPathDetail);
                    }
                    else {
                        if (newAllCodesOne.has(`${sourceCode_three}`) == true && newAllCodesOne.has(`${targetCode_three}`) == true) {
                            newSubResult.path.push(subPathDetail);
                        }
                    }
                }
                if (newSubResult.path.length > 0) {
                    newTempResultThree.push(newSubResult);
                    //过滤重复的path
                    let pathGroup = promiseResultThree.dataDetail.data.pathDetail;
                    let isDupFlag = isDuplicatedPath(pathGroup, newSubResult);
                    if (isDupFlag == false) {
                        promiseResultThree.dataDetail.data.pathDetail.push(newSubResult);
                        for (let newSubPathDetail of newSubResult.path) {
                            uniqueCodesThree.add(newSubPathDetail.sourceCode);
                            uniqueCodesThree.add(newSubPathDetail.targetCode);
                            uniqueNamesThree.add(newSubPathDetail.source);
                            uniqueNamesThree.add(newSubPathDetail.target);
                        }
                    }
                }
            }

            uniqueCodesThreeArray = Array.from(uniqueCodesThree);
            uniqueNamesThreeArray = Array.from(uniqueNamesThree);

            let retryCount = 0;
            do {
                try {
                    if (uniqueCodesOne.length > 0) {
                        allNamesOne = await cacheHandlers.getAllNames(uniqueCodesOne);             //ITCode2->ITName
                    }
                    if (uniqueCodesTwo.length > 0) {
                        allNamesTwo = await cacheHandlers.getAllNames(uniqueCodesTwo);             //ITCode2->ITName
                    }
                    if (uniqueCodesFourArray.length > 0) {
                        allNamesFour = await cacheHandlers.getAllNames(uniqueCodesFourArray);
                    }
                    break;
                } catch (err) {
                    retryCount++;
                }
            } while (retryCount < 3)
            if (retryCount == 3) {
                console.error('retryCount: 3, 批量查询机构名称失败');
                logger.error('retryCount: 3, 批量查询机构名称失败');
            }

            let newAllNamesOne = handlerAllNames(allNamesOne);                         // 处理ITName为空的情况
            let codeNameMapResOne = getCodeNameMapping(uniqueCodesOne, newAllNamesOne);   //获取ITCode2->ITName的Map
            promiseResultOne.mapRes = codeNameMapResOne;
            promiseResultOne.uniqueCodes = uniqueCodesOne;
            promiseResultOne.dataDetail.names = newAllNamesOne;
            promiseResultOne.dataDetail.codes = uniqueCodesOne;
            promiseResultOne.dataDetail.data.pathNum = promiseResultOne.dataDetail.data.pathDetail.length;

            let newAllNamesTwo = handlerAllNames(allNamesTwo);                         // 处理ITName为空的情况
            let codeNameMapResTwo = getCodeNameMapping(uniqueCodesTwo, newAllNamesTwo);   //获取ITCode2->ITName的Map
            promiseResultTwo.mapRes = codeNameMapResTwo;
            promiseResultTwo.uniqueCodes = uniqueCodesTwo;
            promiseResultTwo.dataDetail.names = newAllNamesTwo;
            promiseResultTwo.dataDetail.codes = uniqueCodesTwo;
            promiseResultTwo.dataDetail.data.pathNum = promiseResultTwo.dataDetail.data.pathDetail.length;

            let newAllNamesFour = handlerAllNames(allNamesFour);
            let codeNameMapResFour = getCodeNameMapping(uniqueCodesFourArray, newAllNamesFour);

            promiseResultThree.dataDetail.names = uniqueNamesThreeArray;
            promiseResultThree.dataDetail.codes = uniqueCodesThreeArray;
            // promiseResultThree.dataDetail.data.pathDetail = tempResultThree;
            promiseResultThree.dataDetail.data.pathNum = promiseResultThree.dataDetail.data.pathDetail.length;

            promiseResultFour.mapRes = codeNameMapResFour;
            promiseResultFour.dataDetail.names = uniqueNamesFourArray.concat(newAllNamesFour);
            promiseResultFour.dataDetail.codes = uniqueCodesFourArray;
            promiseResultFour.dataDetail.data.pathNum = promiseResultFour.dataDetail.data.pathDetail.length;

            promiseResult.pathTypeOne = promiseResultOne;
            promiseResult.pathTypeTwo = promiseResultTwo;
            promiseResult.pathTypeThree = promiseResultThree;
            promiseResult.pathTypeFour = promiseResultFour;

            return promiseResult;
        }
    }
}

module.exports = resultHandlers;