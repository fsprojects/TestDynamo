
/** 
 * Copies all files from generate code project into test dynamo project
 * This allows us to keep the generated code separate from the main project, which helps with compile time
 * while also not needing the complexity of deploying 2 dlls into 1 nuget package
 */

const grabFilesAsXml = require("./grabFilesAsXml")
const fs = require("fs")
const path = require("path")

execute();

function argErr() {
    throw new Error('Input args: --testDynamo {file} --generatedCode {file}');
}

async function execute() {

    let args = [...process.argv].slice(2)
    let testDynamo = []
    let generatedCode = []
    for (let i = args.length - 2; i >= 0; i--) {
        if (args[i] === "--testDynamo") {
            testDynamo.push(args[i + 1])
            args.splice(i, 2)
        } else if (args[i] === "--generatedCode") {
            generatedCode.push(args[i + 1])
            args.splice(i, 2)
        }
    }

    if (testDynamo.length !== 1 || generatedCode.length !== 1 || args.length) {
        argErr()
    }

    [testDynamo, generatedCode] = await grabFilesAsXml([testDynamo[0], generatedCode[0]])

    const genCodeDep = testDynamo.dependencies.filter(dep => dep.name === generatedCode.path)[0]
    if (!genCodeDep) throw new Error(`Could not find ${generatedCode.path} dependency in ${testDynamo.name}`)
    genCodeDep.setter(null)

    if (!testDynamo.fileXml.Project.ItemGroup) {
        testDynamo.fileXml.Project.ItemGroup = []
    }

    const files = []
    testDynamo.fileXml.Project.ItemGroup.splice(0, 0, {Compile: files})
    
    for (var i = 0; i < generatedCode.fileXml.Project.ItemGroup.length; i++) {
        const itemGroup = generatedCode.fileXml.Project.ItemGroup[i]
        if (!itemGroup.Compile) continue

        itemGroup.Compile.forEach(compileFile => 
            fs.copyFileSync(
                path.resolve(path.parse(generatedCode.path).dir, compileFile.$.Include),
                path.resolve(path.parse(testDynamo.path).dir, compileFile.$.Include)
            ))

        files.push(...itemGroup.Compile)
    }

    testDynamo.save()
}