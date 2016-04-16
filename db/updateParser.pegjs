{
  // Declared by PEG: input, options, parser, text(), location(), expected(), error()

  var context = options.context
  var attrNames = context.attrNames || Object.create(null)
  var attrVals = context.attrVals || Object.create(null)
  var unusedAttrNames = context.unusedAttrNames || Object.create(null)
  var unusedAttrVals = context.unusedAttrVals || Object.create(null)
  var isReserved = context.isReserved
  var errors = Object.create(null)
  var sections = Object.create(null)
  var paths = []
  var nestedPaths = Object.create(null)

  function checkReserved(name) {
    if (isReserved(name) && !errors.reserved) {
      errors.reserved = 'Attribute name is a reserved keyword; reserved keyword: ' + name
    }
  }

  function checkFunction(name, args) {
    if (errors.unknownFunction) {
      return
    }
    var functions = {
      'if_not_exists': 2,
      'list_append': 2,
      '+': 2,
      '-': 2,
    }
    var numOperands = functions[name]
    if (numOperands == null) {
      errors.unknownFunction = 'Invalid function name; function: ' + name
      return
    }
    if (errors.function) {
      return
    }
    if (numOperands != args.length) {
      errors.function = 'Incorrect number of operands for operator or function; ' +
        'operator or function: ' + name + ', number of operands: ' + args.length
      return
    }
    switch (name) {
      case 'if_not_exists':
        if (!Array.isArray(args[0])) {
          errors.function = 'Operator or function requires a document path; ' +
            'operator or function: ' + name
          return
        }
        return getType(args[1])
      case 'list_append':
        for (var i = 0; i < args.length; i++) {
          var type = getImmediateType(args[i])
          if (type && type != 'L') {
            errors.function = 'Incorrect operand type for operator or function; ' +
              'operator or function: ' + name + ', operand type: ' + type
            return
          }
        }
        return 'L'
      case '+':
      case '-':
        for (var i = 0; i < args.length; i++) {
          var type = getImmediateType(args[i])
          if (type && type != 'N') {
            errors.function = 'Incorrect operand type for operator or function; ' +
              'operator or function: ' + name + ', operand type: ' + type
            return
          }
        }
        return 'N'
    }
  }

  function checkSection(type) {
    if (errors.section) {
      return
    }
    if (sections[type]) {
      errors.section = 'The "' + type + '" section can only be used once in an update expression;'
      return
    }
    sections[type] = true
  }

  function resolveAttrName(name) {
    if (errors.attrName) {
      return
    }
    if (!attrNames[name]) {
      errors.attrName = 'An expression attribute name used in the document path is not defined; attribute name: ' + name
      return
    }
    delete unusedAttrNames[name]
    return attrNames[name]
  }

  function resolveAttrVal(name) {
    if (errors.attrVal) {
      return
    }
    if (!attrVals[name]) {
      errors.attrVal = 'An expression attribute value used in expression is not defined; attribute value: ' + name
      return
    }
    delete unusedAttrVals[name]
    return attrVals[name]
  }

  function checkPath(path) {
    if (errors.pathOverlap || !Array.isArray(path)) {
      return
    }
    for (var i = 0; i < paths.length; i++) {
      checkPaths(paths[i], path)
      if (errors.pathOverlap) {
        return
      }
    }
    paths.push(path)
  }

  function checkPaths(path1, path2) {
    for (var i = 0; i < path1.length && i < path2.length; i++) {
      if (typeof path1[i] !== typeof path2[i]) {
        errors.pathConflict = 'Two document paths conflict with each other; ' +
          'must remove or rewrite one of these paths; path one: ' + pathStr(path1) + ', path two: ' + pathStr(path2)
        return
      }
      if (path1[i] !== path2[i]) return
    }
    if (errors.pathOverlap) {
      return
    }
    errors.pathOverlap = 'Two document paths overlap with each other; ' +
      'must remove or rewrite one of these paths; path one: ' + pathStr(path1) + ', path two: ' + pathStr(path2)
  }

  function pathStr(path) {
    return '[' + path.map(function(piece) {
      return typeof piece == 'number' ? '[' + piece + ']' : piece
    }).join(', ') + ']'
  }

  function checkOperator(operator, val) {
    if (errors.operand || !val) {
      return
    }
    var typeMappings = {
      S: 'STRING',
      N: 'NUMBER',
      B: 'BINARY',
      NULL: 'NULL',
      BOOL: 'BOOLEAN',
      L: 'LIST',
      M: 'MAP',
    }
    var type = getImmediateType(val)
    if (typeMappings[type] && !(operator == 'ADD' && type == 'N')) {
      errors.operand = 'Incorrect operand type for operator or function; operator: ' +
        operator + ', operand type: ' + typeMappings[type]
    }
    return type
  }

  function getType(val) {
    if (!val || typeof val != 'object' || Array.isArray(val)) return null
    if (val.attrType) return val.attrType
    return getImmediateType(val)
  }

  function getImmediateType(val) {
    if (!val || typeof val != 'object' || Array.isArray(val) || val.attrType) return null
    var types = ['S', 'N', 'B', 'NULL', 'BOOL', 'SS', 'NS', 'BS', 'L', 'M']
    for (var i = 0; i < types.length; i++) {
      if (val[types[i]] != null) return types[i]
    }
    return null
  }

  function checkErrors() {
    var errorOrder = ['reserved', 'unknownFunction', 'section', 'attrName',
      'attrVal', 'pathOverlap', 'pathConflict', 'operand', 'function']
    for (var i = 0; i < errorOrder.length; i++) {
      if (errors[errorOrder[i]]) return errors[errorOrder[i]]
    }
    return null
  }
}

Start
  = _ sections:SectionList _ {
      return checkErrors() || {sections: sections, paths: paths, nestedPaths: nestedPaths}
    }

SectionList
  = head:Section tail:(_ sec:Section { return sec })* {
      return [].concat.apply(head, tail)
    }

Section
  = SetToken _ args:SetArgumentList {
      checkSection('SET')
      return args
    }
  / RemoveToken _ args:RemoveArgumentList {
      checkSection('REMOVE')
      return args
    }
  / AddToken _ args:AddArgumentList {
      checkSection('ADD')
      return args
    }
  / DeleteToken _ args:DeleteArgumentList {
      checkSection('DELETE')
      return args
    }

SetArgumentList
  = head:SetExpression tail:(_ ',' _ expr:SetExpression { return expr })* {
      return [head].concat(tail)
    }

RemoveArgumentList
  = head:RemoveExpression tail:(_ ',' _ expr:RemoveExpression { return expr })* {
      return [head].concat(tail)
    }

AddArgumentList
  = head:AddExpression tail:(_ ',' _ expr:AddExpression { return expr })* {
      return [head].concat(tail)
    }

DeleteArgumentList
  = head:DeleteExpression tail:(_ ',' _ expr:DeleteExpression { return expr })* {
      return [head].concat(tail)
    }

SetExpression
  = path:PathExpression _ '=' _ val:SetValueParens {
      checkPath(path)
      return {type: 'set', path: path, val: val, attrType: getType(val)}
    }

RemoveExpression
  = path:PathExpression {
      checkPath(path)
      return {type: 'remove', path: path}
    }

AddExpression
  = path:PathExpression _ val:ExpressionAttributeValue {
      checkPath(path)
      var attrType = checkOperator('ADD', val)
      return {type: 'add', path: path, val: val, attrType: attrType}
    }

DeleteExpression
  = path:PathExpression _ val:ExpressionAttributeValue {
      checkPath(path)
      var attrType = checkOperator('DELETE', val)
      return {type: 'delete', path: path, val: val, attrType: attrType}
    }

SetValueParens
  = '(' _ val:SetValue _ ')' { return val }
  / SetValue

SetValue
  = arg1:OperandParens _ '+' _ arg2:OperandParens {
      var attrType = checkFunction('+', [arg1, arg2])
      return {type: 'add', args: [arg1, arg2], attrType: attrType}
    }
  / arg1:OperandParens _ '-' _ arg2:OperandParens {
      var attrType = checkFunction('-', [arg1, arg2])
      return {type: 'subtract', args: [arg1, arg2], attrType: attrType}
    }
  / OperandParens

OperandParens
  = '(' _ op:Operand _ ')' { return op }
  / Operand

Operand
  = Function
  / ExpressionAttributeValue
  / PathExpression

Function
  = !ReservedWord head:IdentifierStart tail:IdentifierPart* _ '(' _ args:FunctionArgumentList _ ')' {
      var name = head + tail.join('')
      var attrType = checkFunction(name, args)
      return {type: 'function', name: name, args: args, attrType: attrType}
    }

FunctionArgumentList
  = head:OperandParens tail:(_ ',' _ expr:OperandParens { return expr })* {
      return [head].concat(tail)
    }

ExpressionAttributeName
  = !ReservedWord head:'#' tail:IdentifierPart* {
      return resolveAttrName(head + tail.join(''))
    }

ExpressionAttributeValue
  = !ReservedWord head:':' tail:IdentifierPart* {
      return resolveAttrVal(head + tail.join(''))
    }

PathExpression
  = head:Identifier tail:(
      _ '[' _ ix:[0-9]+ _ ']' {
        return +(ix.join(''))
      }
    / _ '.' _ prop:Identifier {
        return prop
      }
    )* {
      var path = [head].concat(tail)
      if (path.length > 1) {
        nestedPaths[path[0]] = true
      }
      return path
    }

Identifier
  = !ReservedWord head:IdentifierStart tail:IdentifierPart* {
      var name = head + tail.join('')
      checkReserved(name)
      return name
    }
  / ExpressionAttributeName

IdentifierStart
  = [a-zA-Z]
  / '_'

IdentifierPart
  = IdentifierStart
  / [0-9]

AttributePart
  = IdentifierPart
  / '#'
  / ':'

ReservedWord
  = SetToken
  / RemoveToken
  / AddToken
  / DeleteToken

SetToken = 'SET'i !AttributePart
RemoveToken = 'REMOVE'i !AttributePart
AddToken = 'ADD'i !AttributePart
DeleteToken = 'DELETE'i !AttributePart

_ 'whitespace'
  = [ \t\r\n]*
