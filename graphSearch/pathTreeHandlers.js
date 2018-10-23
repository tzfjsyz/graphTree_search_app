/*用于Tree结构的生成和排序
wrote by tzf, 2018/4/11
*/
const log4js = require('log4js');
const req = require('require-yml');
const config = req('./config/source.yml');
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

//按数组长度从小到大升序排序
function sortByPathLength(a, b) {
    let flag = a.path.length - b.path.length;                                                               //按path层级升序
    return flag;
}

//向每个subLeaf下插入leaf，担保
function insertToSourceGuarantee(rootCode, source, target, subEachPath) {
    if (target.ITCode != rootCode && source.ITcode != target.ITCode) {                        //如果source不是root,则直接将target -> source放入subLeaf下
        source.subLeaf.push(target);
        source.subLeafNum = source.subLeaf.length;
        // let sortSubLeaf = source.subLeaf.sort(sortRateName);
        // source.subLeaf = sortSubLeaf;
    }
}

//按投资比例, 注册资本, 名称因子排序
function sortRegRateName(a, b) {
    let shareholdRatio_a = a.shareholdRatio;
    let regCapitalRMB_a = a.regCapitalRMB;
    let ITName_a = a.ITName;
    let shareholdRatio_b = b.shareholdRatio;
    let regCapitalRMB_b = b.regCapitalRMB;
    let ITName_b = b.ITName;
    let flag = 0;
    let flagOne = (shareholdRatio_b) * (regCapitalRMB_b) - (shareholdRatio_a) * (regCapitalRMB_a);               //投资比例*注册资本 降序
    let flagTwo = shareholdRatio_b - shareholdRatio_a;                                                           //投资比例 降序
    let flagThree = regCapitalRMB_b - regCapitalRMB_a;                                                           //注册资本 降序
    let flagFour = ITName_a - ITName_b;
    //1. 投资比例*注册资本 = 0
    if (flagOne == 0) {
        flag = flagTwo;                                                                                          //投资比例 降序
        //2. 投资比例 = 0
        if (flag == 0) {
            flag = flagThree;                                                                                    //注册资本 降序
            //3. 注册资本 = 0
            if (flag == 0) {
                flag = flagFour;                                                                                 //公司全称 升序
            }
        }
    }
    else if (flagOne != 0) {
        flag = flagOne;
    }
    return flag;
}

//处理投资关系的leaf节点生成
function insertToSourceInvest(rootCode, source, target, subEachPath) {
    let subEachPathLength = subEachPath.path.length;
    let leafs = source.subLeaf;
    let targetExists = false;
    let sourceExists = false;
    if (source.ITCode == rootCode) {                               //如果source是root,则去遍历root下的cache
        for (let each of leafs) {
            if (each.ITCode == target.ITCode) {
                targetExists = true;
            }
            if (each.ITCode == source.ITCode) {
                sourceExists = true;
            }
        }
    } else if (source.ITCode != rootCode) {                        //如果source不是root,则直接将target -> source放入subLeaf下
        sourceExists = true;
        targetExists = false;
    }

    if (!targetExists) {
        if (!sourceExists) {
            source.subLeaf.push(target);                                 //如果source和target都不存在cache里面，则直接将target的信息放在source的subLeaf下面
        }
        else if (sourceExists) {
            let targetHasITCode = 1;                                         //如果source已经在cache中存在，则按照eachPath下的信息重新给target信息赋值
            if (subEachPath.path[subEachPathLength - 1].targetIsExtra == 1 || subEachPath.path[subEachPathLength - 1].targetIsExtra == '1' || subEachPath.path[subEachPathLength - 1].targetIsExtra == 'null') {
                targetHasITCode = 0;
            }
            target = {
                ITCode: subEachPath.path[subEachPathLength - 1].targetCode,
                ITName: subEachPath.path[subEachPathLength - 1].target,
                regCapitalRMB: subEachPath.path[subEachPathLength - 1].targetRegCapitalRMB,
                regCapital: subEachPath.path[subEachPathLength - 1].targetRegCapital,
                regCapitalUnit: subEachPath.path[subEachPathLength - 1].targetRegCapitalUnit || '万人民币元',
                hasITCode: targetHasITCode,
                isPerson: parseInt(subEachPath.path[subEachPathLength - 1].targetIsPerson),
                shareholdRatio: subEachPath.path[subEachPathLength - 1].shareholdRatio,
                shareholdQuantityRMB: subEachPath.path[subEachPathLength - 1].shareholdQuantityRMB,
                shareholdQuantity: subEachPath.path[subEachPathLength - 1].shareholdQuantity,
                shareholdQuantityUnit: subEachPath.path[subEachPathLength - 1].shareholdQuantityUnit || '万人民币元',
                subLeaf: [],
                subLeafNum: 0
            }
            source.subLeaf.push(target);
        }
        source.subLeafNum = source.subLeaf.length;
        if (source.subLeafNum > 1) {
            //先按照投资比列>= 5%和< 5%归类
            let leafGroup = source.subLeaf;
            let leafGroup_a = [];
            let leafGroup_b = [];
            let sortSubLeaf = [];
            for (let subLeafGroup of leafGroup) {
                let subLeafGroupRatio = subLeafGroup.shareholdRatio;
                if (subLeafGroupRatio >= 5) {
                    leafGroup_a.push(subLeafGroup);
                }
                else if (subLeafGroupRatio < 5) {
                    leafGroup_b.push(subLeafGroup);
                }
            }
            if (leafGroup_a.length > 0) {
                let sortSubLeaf_a = leafGroup_a.sort(sortRegRateName);
                sortSubLeaf = sortSubLeaf_a;
            }
            if (leafGroup_b.length > 0) {
                let sortSubLeaf_b = leafGroup_b.sort(sortRegRateName);
                sortSubLeaf = sortSubLeaf.concat(sortSubLeaf_b);
            }
            source.subLeaf = sortSubLeaf;
        }
    }
}

//按股权比例降序、机构全称升序
function sortRateName(a, b) {
    let flag = b.shareholdRatio - a.shareholdRatio;                                                           //投资比例 降序
    if (flag == 0) {
        flag = a.ITName - b.ITName;                                                                           //公司全称 升序
    }
    return flag;
}

//向每个subLeaf下插入leaf，被担保
function insertToSourceGuaranteedBy(rootCode, source, target, subEachPath) {
    if (target.ITCode != rootCode && source.ITCode != target.ITCode) {                        //如果source不是root,则直接将target -> source放入subLeaf下
        source.subLeaf.push(target);
        source.subLeafNum = source.subLeaf.length;
        // let sortSubLeaf = source.subLeaf.sort(sortRateName);
        // source.subLeaf = sortSubLeaf;
    }
}

//向每个subLeaf下插入leaf，用于investedBy,股东
function insertToSourceInvestedBy(rootCode, source, target, subEachPath) {
    let subEachPathLength = subEachPath.path.length;
    let leafs = source.subLeaf;
    let targetExists = false;
    let sourceExists = false;
    if (source.ITCode == rootCode) {                               //如果source是root,则去遍历root下的cache
        for (let each of leafs) {
            if (each.ITCode == target.ITCode) {
                targetExists = true;
            }
            if (each.ITCode == source.ITCode) {
                sourceExists = true;
            }
        }
    } else if (source.ITCode != rootCode) {                        //如果source不是root,则直接将target -> source放入subLeaf下
        sourceExists = true;
        targetExists = false;
    }

    if (!targetExists) {
        if (!sourceExists) {
            source.subLeaf.push(target);                                 //如果source和target都不存在cache里面，则直接将target的信息放在source的subLeaf下面
        }
        else if (sourceExists) {                                           //如果source已经在cache中存在，则按照eachPath下的信息重新给target信息赋值
            let sourceHasITCode = 1;
            if (subEachPath.path[subEachPathLength - 1].sourceIsExtra == 1 || subEachPath.path[subEachPathLength - 1].sourceIsExtra == '1' || subEachPath.path[subEachPathLength - 1].sourceIsExtra == 'null') {
                sourceHasITCode = 0;
            }
            target = {
                ITCode: subEachPath.path[subEachPathLength - 1].sourceCode,
                ITName: subEachPath.path[subEachPathLength - 1].source,
                regCapitalRMB: subEachPath.path[subEachPathLength - 1].sourceRegCapitalRMB,
                regCapital: subEachPath.path[subEachPathLength - 1].sourceRegCapital,
                regCapitalUnit: subEachPath.path[subEachPathLength - 1].sourceRegCapitalUnit || '万人民币元',
                hasITCode: sourceHasITCode,
                isPerson: parseInt(subEachPath.path[subEachPathLength - 1].sourceIsPerson),
                shareholdRatio: subEachPath.path[subEachPathLength - 1].shareholdRatio,
                shareholdQuantityRMB: subEachPath.path[subEachPathLength - 1].shareholdQuantityRMB,
                shareholdQuantity: subEachPath.path[subEachPathLength - 1].shareholdQuantity,
                shareholdQuantityUnit: subEachPath.path[subEachPathLength - 1].shareholdQuantityUnit || '万人民币元',
                subLeaf: [],
                subLeafNum: 0
            }
            source.subLeaf.push(target);
        }

        // (source.subLeaf).sort(sortRegRateName);
        source.subLeafNum = source.subLeaf.length;
        if (source.subLeafNum > 1) {
            let sortSubLeaf = source.subLeaf.sort(sortRateName);                            //按持股比例排序
            source.subLeaf = sortSubLeaf;
            //for sort results test
            // for (let i = 0; i < source.subLeaf.length; i++) {
            //     let shareholdRatio = source.subLeaf[i].shareholdRatio;
            //     console.log(`index ${i} shareholdRatio: ` +shareholdRatio);
            //     logger.info(`index ${i} shareholdRatio: ` +shareholdRatio);
            // }
        }
    }
}

let pathTreeHandlers = {

    //根据nodeResult创建Tree, invest
    fromTreePath1: function (nodeResult, rootITCode, relation) {
        let cache = {};
        let firstSortByPathLengthStart = Date.now();
        let sortNodeResult = nodeResult.sort(sortByPathLength);                           //将nodeResult按path的层级升序排序
        let firstSortByPathLengthCost = Date.now() - firstSortByPathLengthStart;
        console.log('firstSortByPathLengthCost: ' + firstSortByPathLengthCost + 'ms');
        for (let each of sortNodeResult) {
            let rootCode = each.path[0].sourceCode;                                     //每个path下的root节点的ITCode    
            let eachPathDepth = each.path.length;                                        //每个path的层级数   
            for (let eachPath of each.path) {
                let source = cache[eachPath.sourceCode];                                 //通过key(每个节点的sourceCode)查找value
                let sourceHasITCode = 1;
                let targetHasITCode = 1;
                let sourceIsPerson = 0;
                let targetIsPerson = 0;
                //判断是否有机构代码
                if (eachPath.sourceIsExtra == 1 || eachPath.sourceIsExtra == '1' || eachPath.sourceIsExtra == 'null') {
                    sourceHasITCode = 0;
                }
                if (eachPath.targetIsExtra == 1 || eachPath.targetIsExtra == '1' || eachPath.targetIsExtra == 'null') {
                    targetHasITCode = 0;
                }
                //判断是否自然人
                if (eachPath.sourceIsPerson == 1 || eachPath.sourceIsPerson == '1') {
                    sourceIsPerson = 1;
                }
                if (eachPath.targetIsPerson == 1 || eachPath.targetIsPerson == '1') {
                    targetIsPerson = 1;
                }
                if (!source) {
                    source = {
                        ITCode: eachPath.sourceCode,
                        ITName: eachPath.source,
                        regCapitalRMB: eachPath.sourceRegCapitalRMB,
                        regCapital: eachPath.sourceRegCapital,
                        regCapitalUnit: eachPath.sourceRegCapitalUnit || '万人民币元',
                        hasITCode: sourceHasITCode,
                        isPerson: sourceIsPerson,
                        subLeaf: [],
                        subLeafNum: 0
                    };
                    cache[eachPath.sourceCode] = source;                               //将source结构的信息作为value存入到key为sourceCode的cache中
                }
                let target = cache[eachPath.targetCode];
                if (!target) {
                    target = {
                        ITCode: eachPath.targetCode,
                        ITName: eachPath.target,
                        regCapitalRMB: eachPath.targetRegCapitalRMB,
                        regCapital: eachPath.targetRegCapital,
                        regCapitalUnit: eachPath.targetRegCapitalUnit || '万人民币元',
                        hasITCode: targetHasITCode,
                        isPerson: targetIsPerson,
                        shareholdRatio: eachPath.shareholdRatio,
                        shareholdQuantityRMB: eachPath.shareholdQuantityRMB,
                        shareholdQuantity: eachPath.shareholdQuantity,
                        shareholdQuantityUnit: eachPath.shareholdQuantityUnit || '万人民币元',
                        subLeaf: [],
                        subLeafNum: 0
                    };
                    cache[eachPath.targetCode] = target;
                }
                if (relation == 'guarantees') {
                    insertToSourceGuarantee(rootCode, source, target, each);
                }
                else {
                    if (eachPathDepth == 1) {
                        insertToSourceInvest(rootCode, source, target, each);
                    }
                    if (eachPathDepth > 1 && source.ITCode != rootCode && target.ITCode != rootCode) {
                        insertToSourceInvest(rootCode, source, target, each);
                    }
                }
            }
        }
        if (relation == 'guarantees') {
            cache[rootITCode].subLeaf = (cache[rootITCode].subLeaf).sort(sortRateName);
        }
        return cache[rootITCode];
    },

    //根据nodeResult创建Tree, investedBy,股东关系
    fromTreePath2: function (nodeResult, rootITCode, relation) {
        let cache = {};
        let firstSortByPathLengthStart = Date.now();
        let sortNodeResult = nodeResult.sort(sortByPathLength);                          //将nodeResult按path的层级升序排序
        let firstSortByPathLengthCost = Date.now() - firstSortByPathLengthStart;
        console.log('firstSortByPathLengthCost: ' + firstSortByPathLengthCost + 'ms');

        for (let each of sortNodeResult) {
            let rootCode = each.path[0].targetCode;                                      //每个path下的root节点的ITCode
            let eachPathDepth = each.path.length;                                        //每个path的层级数
            for (let eachPath of each.path) {
                let source = cache[eachPath.targetCode];                                 //通过投资的ITCode2作为key, 查找cache中对应的value值作为被投资的企业信息
                let sourceHasITCode = 1;
                let targetHasITCode = 1;
                let sourceIsPerson = 0;
                let targetIsPerson = 0;
                //判断是否有机构代码
                if (eachPath.sourceIsExtra == 1 || eachPath.sourceIsExtra == '1' || eachPath.sourceIsExtra == 'null') {
                    sourceHasITCode = 0;
                }
                if (eachPath.targetIsExtra == 1 || eachPath.targetIsExtra == '1' || eachPath.targetIsExtra == 'null') {
                    targetHasITCode = 0;
                }
                //判断是否自然人
                if (eachPath.sourceIsPerson == 1 || eachPath.sourceIsPerson == '1') {
                    sourceIsPerson = 1;
                }
                if (eachPath.targetIsPerson == 1 || eachPath.targetIsPerson == '1') {
                    targetIsPerson = 1;
                }
                if (!source) {
                    source = {
                        ITCode: eachPath.targetCode,
                        ITName: eachPath.target,
                        regCapitalRMB: eachPath.targetRegCapitalRMB,
                        regCapital: eachPath.targetRegCapital,
                        regCapitalUnit: eachPath.targetRegCapitalUnit || '万人民币元',
                        hasITCode: targetHasITCode,
                        isPerson: targetIsPerson,
                        subLeaf: [],
                        subLeafNum: 0
                    };
                    cache[eachPath.targetCode] = source;
                }
                let target = cache[eachPath.sourceCode];                               //仅仅是从cache里面通过ITCode查找
                //target存在，继续核对target上面的属性
                // let existTargetInDirectSource = directSource[eachPath.sourceCode];
                // if (eachPathDepth > 1  &&  existTargetInDirectSource) {
                //     target = existTargetInDirectSource;                               //如果该target在directSource里存在，则直接使用directSource里面查出来的target信息
                // }
                if (!target) {
                    target = {
                        ITCode: eachPath.sourceCode,
                        ITName: eachPath.source,
                        regCapitalRMB: eachPath.sourceRegCapitalRMB,
                        regCapital: eachPath.sourceRegCapital,
                        regCapitalUnit: eachPath.sourceRegCapitalUnit || '万人民币元',
                        hasITCode: sourceHasITCode,
                        isPerson: sourceIsPerson,
                        shareholdRatio: eachPath.shareholdRatio,
                        shareholdQuantityRMB: eachPath.shareholdQuantityRMB,
                        shareholdQuantity: eachPath.shareholdQuantity,
                        shareholdQuantityUnit: eachPath.shareholdQuantityUnit || '万人民币元',
                        subLeaf: [],
                        subLeafNum: 0
                    };
                    cache[eachPath.sourceCode] = target;
                }
                // if (source.ITCode != target.ITCode && source.ITCode != rootCode && target.ITCode != rootCode) {
                //    insertToSourceInvestedBy(source,target);
                // }

                // let lastSourceCode = each.path[eachPathDepth - 1].sourceCode;
                // let lastTargetCode = each.path[eachPathDepth - 1].targetCode;
                if (relation == 'guarantees') {
                    insertToSourceGuaranteedBy(rootCode, source, target, each);
                }
                else {
                    if (eachPathDepth == 1) {
                        insertToSourceInvestedBy(rootCode, source, target, each);
                    }
                    // else if (eachPathDepth > 1 && lastSourceCode != rootCode) {                              //过滤闭包的路径
                    //     insertToSourceInvestedBy(rootCode, source, target, each);
                    // }
                    else if (eachPathDepth > 1 && source.ITCode != rootCode && target.ITCode != rootCode) {
                        insertToSourceInvestedBy(rootCode, source, target, each);
                    }
                }
            }
        }
        if (relation == 'guarantees' && cache[rootITCode]) {
            cache[rootITCode].subLeaf = (cache[rootITCode].subLeaf).sort(sortRateName);
        }
        return cache[rootITCode];
    }
}

module.exports = pathTreeHandlers;