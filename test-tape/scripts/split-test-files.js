const fs = require('fs')
const path = require('path')
const recast = require('recast')
const b = recast.types.builders

const SOURCE_DIR = path.resolve(__dirname, './mocha-source') // Relative to script in test-tape
const TARGET_DIR = path.resolve(__dirname, './mocha-source-split') // Relative to script in test-tape
const MAX_LINES = 500

function getLineCount (node) {
  if (!node || !node.loc) return 0
  if (node.loc.start.line === node.loc.end.line && node.loc.start.column === node.loc.end.column) {
    return recast.print(node).code.split('\n').length
  }
  return node.loc.end.line - node.loc.start.line + 1
}

function splitFile (filePath) {
  const originalCode = fs.readFileSync(filePath, 'utf8')
  const ast = recast.parse(originalCode, { tolerant: true })
  const body = ast.program.body

  const headerNodes = []
  const topLevelDescribeNodes = []
  const otherTopLevelNodes = []

  let isHeader = true
  for (const node of body) {
    let isTopLevelDescribe = node.type === 'ExpressionStatement' &&
                                node.expression.type === 'CallExpression' &&
                                node.expression.callee.name === 'describe'

    if (isHeader &&
            ( (node.type === 'VariableDeclaration' && (node.kind === 'var' || node.kind === 'const')) ||
              (node.type === 'ExpressionStatement' && node.expression.type === 'CallExpression' && node.expression.callee.name === 'require') ||
              (node.type === 'ExpressionStatement' && node.expression.type === 'AssignmentExpression') ||
              (node.type.endsWith('ImportDeclaration'))
            )
    ) {
      headerNodes.push(node)
    }
    else if (isTopLevelDescribe) {
      isHeader = false
      topLevelDescribeNodes.push(node)
    }
    else {
      isHeader = false
      otherTopLevelNodes.push(node)
    }
  }

  let partIndex = 1
  const generatedFiles = []

  for (const describeNode of topLevelDescribeNodes) {
    const describeArgs = describeNode.expression.arguments
    if (describeArgs.length < 2 || describeArgs[1].type !== 'FunctionExpression') continue

    const describeBody = describeArgs[1].body.body
    const describeHeaderStr = recast.print(describeNode.expression.callee).code + '(' + recast.print(describeArgs[0]).code + ', function() {\n'
    const describeFooterStr = '\n});'

    let currentPartNodes = []
    let currentLineCount = getLineCount(b.program([ ...headerNodes, ...otherTopLevelNodes ]))
    currentLineCount += describeHeaderStr.split('\n').length
    currentLineCount += describeFooterStr.split('\n').length

    for (const itNode of describeBody) {
      const itNodeLineCount = getLineCount(itNode)

      let isNestedDescribe = itNode.type === 'ExpressionStatement' &&
                                  itNode.expression.type === 'CallExpression' &&
                                  itNode.expression.callee.name === 'describe'

      if (currentPartNodes.length > 0 &&
                ( (currentLineCount + itNodeLineCount > MAX_LINES) || isNestedDescribe) ) {
        generatedFiles.push(writePartNested(filePath, partIndex, headerNodes, otherTopLevelNodes, describeNode, currentPartNodes))
        partIndex++
        currentPartNodes = []
        currentLineCount = getLineCount(b.program([ ...headerNodes, ...otherTopLevelNodes ])) + describeHeaderStr.split('\n').length + describeFooterStr.split('\n').length
      }

      currentPartNodes.push(itNode)
      currentLineCount += itNodeLineCount
    }

    if (currentPartNodes.length > 0) {
      generatedFiles.push(writePartNested(filePath, partIndex, headerNodes, otherTopLevelNodes, describeNode, currentPartNodes))
      partIndex++
    }
  }

  if (topLevelDescribeNodes.length === 0 && (headerNodes.length > 0 || otherTopLevelNodes.length > 0)) {
    console.log(`File ${path.basename(filePath)} has no top-level describe blocks to split or is already small.`)
    return []
  }

  console.log(`Split ${path.basename(filePath)} into ${partIndex - 1} part(s).`)
  return generatedFiles
}

function writePartNested (originalFilePath, partIndex, headerNodes, otherTopLevelNodes, describeNode, itNodes) {
  const baseName = path.basename(originalFilePath, '.js')
  const newFileName = `${baseName}.part${partIndex}.js`
  const newFilePath = path.join(TARGET_DIR, newFileName)

  const newDescribeNode = recast.parse(recast.print(describeNode).code).program.body[0]
  newDescribeNode.expression.arguments[1].body.body = itNodes

  const allNodes = [ ...headerNodes, ...otherTopLevelNodes, newDescribeNode ]

  const newAst = b.program(allNodes)
  const newCode = recast.print(newAst).code

  fs.writeFileSync(newFilePath, newCode, 'utf8')
  return newFilePath
}

// --- Main Execution ---
console.log(`Reading from: ${SOURCE_DIR}`)
console.log(`Writing to: ${TARGET_DIR}`)
const allGeneratedFiles = []

fs.mkdirSync(TARGET_DIR, { recursive: true })

fs.readdirSync(SOURCE_DIR).forEach(file => {
  const filePath = path.join(SOURCE_DIR, file) // Define filePath here
  if (path.extname(file) === '.js' && file !== 'helpers.js') {
    // const stats = fs.statSync(filePath); // Variable stats is declared but its value is never read.
    const lineCount = fs.readFileSync(filePath, 'utf8').split('\n').length

    if (lineCount > MAX_LINES) {
      console.log(`Splitting ${file} (${lineCount} lines)...`)
      try {
        const generated = splitFile(filePath) // Pass filePath
        allGeneratedFiles.push(...generated)
      }
      catch (error) {
        console.error(`Error splitting file ${file}:`, error)
      }
    }
    else {
      const targetPath = path.join(TARGET_DIR, file)
      fs.copyFileSync(filePath, targetPath)
      console.log(`Copied ${file} (${lineCount} lines)`)
      allGeneratedFiles.push(targetPath)
    }
  }
})

console.log('\n--- Generated File Line Counts ---')
allGeneratedFiles.forEach(filePath => {
  try {
    const lineCount = fs.readFileSync(filePath, 'utf8').split('\n').length
    console.log(`${path.basename(filePath)}: ${lineCount}`)
  }
  catch (err) {
    console.log(`${path.basename(filePath)}: Error reading file`)
  }
})

console.log('\nFile splitting and copying complete.')
