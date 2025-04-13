const fs = require('fs');
const path = require('path');
const recast = require('recast');
const b = recast.types.builders;

const TARGET_DIR = path.join(__dirname, '../test-tape/mocha-source-split');
const MAX_LINES = 500;

function getLineCount(node) {
    if (!node || !node.loc) return 0;
    return node.loc.end.line - node.loc.start.line + 1;
}

function splitFile(filePath) {
    const originalCode = fs.readFileSync(filePath, 'utf8');
    const ast = recast.parse(originalCode);
    const body = ast.program.body;

    const headerNodes = [];
    const topLevelBlocks = []; // describe or it blocks
    let currentBlock = null;

    // Separate header (requires, top-level vars) from test blocks
    body.forEach(node => {
        if (node.type === 'ExpressionStatement' &&
            node.expression.type === 'CallExpression' &&
            node.expression.callee &&
            (node.expression.callee.name === 'describe' || node.expression.callee.name === 'it')) {
            topLevelBlocks.push(node);
        } else if (node.type === 'VariableDeclaration' && node.kind === 'var') {
            // Keep simple var declarations in header
            headerNodes.push(node);
        } else if (node.type === 'ExpressionStatement' && 
                   node.expression.type === 'AssignmentExpression' &&
                   node.expression.left.type === 'Identifier') {
             // Keep simple top-level assignments (like `target = '...'`)
             headerNodes.push(node);
        } else if (node.type === 'VariableDeclaration') {
            // Assume other var declarations are part of header too
            headerNodes.push(node);
        } else {
            // Default to header
            headerNodes.push(node);
        }
    });

    let partIndex = 1;
    let currentPartNodes = [...headerNodes];
    let currentLineCount = headerNodes.reduce((sum, node) => sum + getLineCount(node), 0);

    for (const block of topLevelBlocks) {
        const blockLineCount = getLineCount(block);

        if (currentLineCount > 0 && currentLineCount + blockLineCount > MAX_LINES) {
            // Write current part
            writePart(filePath, partIndex, currentPartNodes);
            partIndex++;
            // Start new part with header
            currentPartNodes = [...headerNodes];
            currentLineCount = headerNodes.reduce((sum, node) => sum + getLineCount(node), 0);
        }

        currentPartNodes.push(block);
        currentLineCount += blockLineCount;
    }

    // Write the last part if it has content beyond the header
    if (currentPartNodes.length > headerNodes.length) {
        writePart(filePath, partIndex, currentPartNodes);
    }

    // Delete original file after successful splitting
    fs.unlinkSync(filePath);
    console.log(`Split and removed original file: ${path.basename(filePath)}`);

}

function writePart(originalFilePath, partIndex, nodes) {
    const baseName = path.basename(originalFilePath, '.js');
    const dirName = path.dirname(originalFilePath);
    const newFileName = `${baseName}.part${partIndex}.js`;
    const newFilePath = path.join(dirName, newFileName);

    const newAst = b.program(nodes);
    const newCode = recast.print(newAst).code;

    fs.writeFileSync(newFilePath, newCode, 'utf8');
    console.log(`Created part: ${newFileName}`);
}

// --- Main Execution ---
fs.readdirSync(TARGET_DIR).forEach(file => {
    if (path.extname(file) === '.js') {
        const filePath = path.join(TARGET_DIR, file);
        const stats = fs.statSync(filePath);
        const lineCount = fs.readFileSync(filePath, 'utf8').split('\n').length;

        if (lineCount > MAX_LINES && file !== 'helpers.js') { // Exclude helpers for now
            console.log(`Splitting ${file} (${lineCount} lines)...`);
            try {
                splitFile(filePath);
            } catch (error) {
                console.error(`Error splitting file ${file}:`, error);
            }
        }
    }
});

console.log("\nFile splitting complete."); 