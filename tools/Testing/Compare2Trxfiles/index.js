
// app too compare 2 trx test results files by time

const fs = require("fs")
const DOMParser = require('xmldom').DOMParser;
const xpath = require('xpath');

console.log("Starting")

const slow = parse(process.argv[2])
const fast = parse(process.argv[3])

console.log(processResults(combine(slow, fast), x => x.name.startsWith("DynamoDbInMemory.Tests.FilterSyntaxTests.Filter, <,<=,>,>=, returns correct items")))

function processResults(combinations, filter) {
    const missingFromSlow = toArray(combinations.missingFromSlow).reduce((s, x) => s + x.value.result, 0)
    const missingFromFast = toArray(combinations.missingFromFast).reduce((s, x) => s + x.value.result, 0)
    const processedDiffs = [...combinations.diffs]
        .sort((x, y) => y.diffAsAPercentageOfSlow - x.diffAsAPercentageOfSlow)
        .filter(filter || (x => true))
    
    return {
        ["Time taken processing tests in fast which are not present in slow"]: missingFromSlow,
        ["Time taken processing tests in slow which are not present in fast"]: missingFromFast,
        ["Filtered test diffs ordered by diffAsAPercentageOfSlow"]: processedDiffs
    }
}

function toArray(x) {
    return Object
        .keys(x)
        .map(key => ({key, value: x[key]}))
}

function combine(slow, fast) {

    const result1 = Object
        .keys(slow)
        .reduce((s, k) => {
            const slowResult = slow[k].seconds
            const fastResult = fast[k]?.seconds

            s.done[k] = true
            if (fastResult == null) {
                s.missingFromFast.push({name: k, result: slow[k].seconds})
                return s
            }

            const diff = slowResult - fastResult
            s.diffs.push({
                name: k,
                diffAsAPercentageOfSlow: diff * 100 / slowResult,
                diff,
                slowResult,
                fastResult,
                slowStart: slow[k].startTime,
                fastStart: fast[k].startTime
            })

            return s
        }, {
            missingFromSlow: [],
            missingFromFast: [],
            diffs: [],
            done: {}
        })

    const result2 = Object
        .keys(fast)
        .reduce((s, k) => {
            if (!s.done[k]) {
                s.done[k] = true
                s.missingFromSlow.push({name: k, result: fast[k].seconds})
            }

            return s
        }, result1)

    return {
        diffs: result2.diffs,
        missingFromSlow: result2.missingFromSlow,
        missingFromFast: result2.missingFromFast
    }
}

function parseDuration(d) {
    const parts = d.split(/:/g)
    if (parts.length !== 3) throw d

    const result = parseFloat(parts[2]) + (parseInt(parts[1]) * 60) + (parseInt(parts[0]) * 60 * 60)
    if (isNaN(result)) throw `Invalid time ${d}`
    return result;
}

function parse(fileName) {
    const parser = new DOMParser();
    const xml = parser.parseFromString(fs.readFileSync(fileName).toString(), "text/xml");
    const select = xpath.useNamespaces({"t": "http://microsoft.com/schemas/VisualStudio/TeamTest/2010"});
    
    return map(select("//t:UnitTestResult", xml), x => ({
        name: select("@testName", x)[0].value,
        startTime: new Date(select("@startTime", x)[0].value),
        duration: select("@duration", x)[0].value,
        seconds: parseDuration(select("@duration", x)[0].value)
    }))
    .reduce((s, x) => {
        if (s[x.name]) throw `Duplicate test name ${x.name}`
        s[x.name] = x
        return s
    }, {})
}


function filter(arr, predicate) {
    return Array.prototype.filter.call(arr, predicate)
}

function map(arr, f) {
    return Array.prototype.map.call(arr, f)
}