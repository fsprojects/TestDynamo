const grabFilesAsXml = require("./grabFilesAsXml")

execute();

async function execute() {

    let args = [...process.argv].slice(2)
    const vFlag = args.indexOf("--version")
    if (vFlag === -1 || vFlag >= args.length) {
        throw new Error('Input args: --version {version} --ignore {ignoreFile1} --ignore {ignoreFile2} --remove {removeFile1} --remove {removeFile1} {file1} {file2}...');
    }

    const packageVersion = args[vFlag + 1]
    args.splice(vFlag, 2)

    const removeFiles = []
    const ignoreFiles = []
    for (let i = args.length - 2; i >= 0; i--) {
        if (args[i] === "--ignore") {
            ignoreFiles.push(args[i + 1])
            args.splice(i, 2)
        } else if (args[i] === "--remove") {
            removeFiles.push(args[i + 1])
            args.splice(i, 2)
        }
    }

    const grouped = args
        .concat(ignoreFiles)
        .concat(removeFiles)
        .reduce((state, file) => state[file] 
            ? {...state, [file]: state[file] + 1}
            : {...state, [file]: 1}, {})

    const duplicates = Object
        .keys(grouped)
        .filter(k => grouped[k] > 1)

    if (duplicates.length) {
        throw new Error(`Duplicate file, ignored or removed: ${duplicates}`);
    }

    const files = await grabFilesAsXml(args, packageVersion)
    if (!files.length) {
        return
    }

    processFiles(files, ignoreFiles, removeFiles)
    files.forEach(file => file.save())
}

function remainingFiles(files) {
    return files
        .map(f => f.packageName + ": " + f.dependencies.reduce((s ,x) => [...s, x.name], []).join(", "))
        .join("\n")
}

function processFiles(files, ignoreFiles, removeFiles) {
    if (!files.length) return

    const head = files
        .filter(x => !x.dependencies
                .filter(dep => ignoreFiles.indexOf(dep.name) === -1
                    && removeFiles.indexOf(dep.name) === -1).length)[0]

    if (!head) throw new Error("Not all dependencies are contained in tree\n" + remainingFiles(files))
    const tail = files.filter(x => x !== head)

    head.dependencies.forEach(dep => {
        if (removeFiles.indexOf(dep.name) !== -1) {
            console.log(`Removing dependency ${dep.name} from ${head.path}`)
            dep.setter(null)
        }
    })

    if (head.packageName) {
        tail.forEach(dependant => {
            for (let i = dependant.dependencies.length - 1; i >= 0; i--) {
                const dep = dependant.dependencies[i]
                if (dep.name === head.path) {
                    dep.setter(head.packageName)
                    dependant.dependencies.splice(i, 1)
                }
            }
        })
    } else {
        console.warn(`Skipping project ${head.path} with no package name`);
    }

    processFiles(tail, ignoreFiles)
}