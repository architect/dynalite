{
  // Declared by PEG: input, options, parser, text(), location(), expected(), error()

  var context = options.context
  var attrNames = context.attrNames || {}
  var attrVals = context.attrVals || {}
  var unusedAttrNames = context.unusedAttrNames || {}
  var unusedAttrVals = context.unusedAttrVals || {}
  var isReserved = context.isReserved
  var errors = Object.create(null)
  var nestedPaths = Object.create(null)
  var pathHeads = Object.create(null)

  function checkReserved(name) {
    if (isReserved(name) && !errors.reserved) {
      errors.reserved = 'Attribute name is a reserved keyword; reserved keyword: ' + name
    }
  }

  function resolveAttrName(name) {
    if (errors.attrNameVal) {
      return
    }
    if (!attrNames[name]) {
      errors.attrNameVal = 'An expression attribute name used in the document path is not defined; attribute name: ' + name
      return
    }
    delete unusedAttrNames[name]
    return attrNames[name]
  }

  function resolveAttrVal(name) {
    if (errors.attrNameVal) {
      return
    }
    if (!attrVals[name]) {
      errors.attrNameVal = 'An expression attribute value used in expression is not defined; attribute value: ' + name
      return
    }
    delete unusedAttrVals[name]
    return attrVals[name]
  }

  function checkFunction(name, args) {
    if (errors.unknownFunction) {
      return
    }
    var functions = {
      attribute_exists: 1,
      attribute_not_exists: 1,
      attribute_type: 2,
      begins_with: 2,
      contains: 2,
      size: 1,
    }
    var numOperands = functions[name]
    if (numOperands == null) {
      errors.unknownFunction = 'Invalid function name; function: ' + name
      return
    }

    if (errors.operand) {
      return
    }
    if (numOperands != args.length) {
      errors.operand = 'Incorrect number of operands for operator or function; ' +
        'operator or function: ' + name + ', number of operands: ' + args.length
      return
    }

    checkDistinct(name, args)

    if (errors.function) {
      return
    }
    switch (name) {
      case 'attribute_exists':
      case 'attribute_not_exists':
        if (!Array.isArray(args[0])) {
          errors.function = 'Operator or function requires a document path; ' +
            'operator or function: ' + name
          return
        }
        return getType(args[1])
      case 'begins_with':
        for (var i = 0; i < args.length; i++) {
          var type = getType(args[i])
          if (type && type != 'S' && type != 'B') {
            errors.function = 'Incorrect operand type for operator or function; ' +
              'operator or function: ' + name + ', operand type: ' + type
            return
          }
        }
        return 'BOOL'
      case 'attribute_type':
        var type = getType(args[1])
        if (type != 'S') {
          if (type == null) type = '{NS,SS,L,BS,N,M,B,BOOL,NULL,S}'
          errors.function = 'Incorrect operand type for operator or function; ' +
            'operator or function: ' + name + ', operand type: ' + type
          return
        }
        if (!~['S', 'N', 'B', 'NULL', 'SS', 'BOOL', 'L', 'BS', 'NS', 'M'].indexOf(args[1].S)) {
          errors.function = 'Invalid attribute type name found; type: ' +
            args[1].S + ', valid types: {B,NULL,SS,BOOL,L,BS,N,NS,S,M}'
          return
        }
        return 'BOOL'
      case 'size':
        var type = getType(args[0])
        if (~['N', 'BOOL', 'NULL'].indexOf(type)) {
          errors.function = 'Incorrect operand type for operator or function; ' +
            'operator or function: ' + name + ', operand type: ' + type
          return
        }
        return 'N'
      case 'contains':
        return 'BOOL'
    }
  }

  function redundantParensError() {
    if (!errors.parens) {
      errors.parens = 'The expression has redundant parentheses;'
    }
  }

  function checkMisusedFunction(args) {
    if (errors.misusedFunction) {
      return
    }
    for (var i = 0; i < args.length; i++) {
      if (args[i] && args[i].type == 'function' && args[i].name != 'size') {
        errors.misusedFunction = 'The function is not allowed to be used this way in an expression; function: ' +
          args[i].name
        return
      }
    }
  }

  function checkMisusedSize(expr) {
    if (expr.type == 'function' && expr.name == 'size' && !errors.misusedFunction) {
      errors.misusedFunction = 'The function is not allowed to be used this way in an expression; function: ' + expr.name
    }
  }

  function checkDistinct(name, args) {
    if (errors.distinct || args.length != 2 || !Array.isArray(args[0]) || !Array.isArray(args[1]) || args[0].length != args[1].length) {
      return
    }
    for (var i = 0; i < args[0].length; i++) {
      if (args[0][i] !== args[1][i]) {
        return
      }
    }
    errors.distinct = 'The first operand must be distinct from the remaining operands for this operator or function; operator: ' +
      name + ', first operand: ' + pathStr(args[0])
  }

  function checkBetweenArgs(x, y) {
    if (errors.function) {
      return
    }
    var type1 = getImmediateType(x)
    var type2 = getImmediateType(y)
    if (type1 && type2) {
      if (type1 != type2) {
        errors.function = 'The BETWEEN operator requires same data type for lower and upper bounds; ' +
          'lower bound operand: AttributeValue: {' + type1 + ':' + x[type1] + '}, ' +
          'upper bound operand: AttributeValue: {' + type2 + ':' + y[type2] + '}'
      } else if (context.compare('GT', x, y)) {
        errors.function = 'The BETWEEN operator requires upper bound to be greater than or equal to lower bound; ' +
          'lower bound operand: AttributeValue: {' + type1 + ':' + x[type1] + '}, ' +
          'upper bound operand: AttributeValue: {' + type2 + ':' + y[type2] + '}'
      }
    }
  }

  function pathStr(path) {
    return '[' + path.map(function(piece) {
      return typeof piece == 'number' ? '[' + piece + ']' : piece
    }).join(', ') + ']'
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

  function checkConditionErrors() {
    if (errors.condition) {
      return
    }
    var errorOrder = ['attrNameVal', 'operand', 'distinct', 'function']
    for (var i = 0; i < errorOrder.length; i++) {
      if (errors[errorOrder[i]]) {
        errors.condition = errors[errorOrder[i]]
        return
      }
    }
  }

  function checkErrors() {
    var errorOrder = ['parens', 'unknownFunction', 'misusedFunction', 'reserved', 'condition']
    for (var i = 0; i < errorOrder.length; i++) {
      if (errors[errorOrder[i]]) return errors[errorOrder[i]]
    }
    return null
  }
}

// XXX: We should really refactor this to just construct the expression tree first,
//      and then traverse to check errors afterwards

Start
  = _ expr:OrConditionExpression _ {
      checkMisusedSize(expr)
      return checkErrors() || {expression: expr, nestedPaths: nestedPaths, pathHeads: pathHeads}
    }

OrConditionExpression
  = x:AndConditionExpression _ token:OrToken _ y:OrConditionExpression {
      [x, y].forEach(checkMisusedSize)
      return {type: 'or', args: [x, y]}
    }
  / expr:AndConditionExpression

AndConditionExpression
  = x:NotConditionExpression _ AndToken _ y:AndConditionExpression {
      [x, y].forEach(checkMisusedSize)
      return {type: 'and', args: [x, y]}
    }
  / NotConditionExpression

NotConditionExpression
  = token:NotToken _ expr:ParensConditionExpression {
      checkMisusedSize(expr)
      return {type: 'not', args: [expr]}
    }
  / ParensConditionExpression

ParensConditionExpression
  = '(' _ '(' _ expr:OrConditionExpression _ ')' _ ')' {
      redundantParensError()
      return expr
    }
  / '(' _ '(' _ expr:ConditionExpression _ ')' _ ')' {
      redundantParensError()
      checkConditionErrors()
      return expr
    }
  / expr:ConditionExpression {
      checkConditionErrors()
      return expr
    }
  / '(' _ expr:OrConditionExpression _ ')' {
      return expr
    }

ConditionExpression
  = x:OperandParens _ comp:Comparator _ y:OperandParens {
      checkMisusedFunction([x, y])
      checkDistinct(comp, [x, y])
      return {type: comp, args: [x, y]}
    }
  / x:OperandParens _ BetweenToken _ y:OperandParens _ AndToken _ z:OperandParens {
      checkMisusedFunction([x, y, z])
      checkBetweenArgs(y, z)
      return {type: 'between', args: [x, y, z]}
    }
  / x:OperandParens _ token:InToken _ '(' _ args:FunctionArgumentList _ ')' {
      checkMisusedFunction([x].concat(args))
      return {type: 'in', args: [x].concat(args)}
    }
  / f:Function

Comparator
  = '>=' / '<=' / '<>' / '=' / '<' / '>'

OperandParens
  = '(' _ '(' _ op:Operand _ ')' _ ')' {
      redundantParensError()
      return op
    }
  / '(' _ op:Operand _ ')' {
      return op
    }
  / Operand

Operand
  = Function
  / ExpressionAttributeValue
  / path:PathExpression {
      for (var i = 0; i < path.length; i++) {
        if (typeof path[i] === 'string') {
          checkReserved(path[i])
        } else if (path[i] && path[i].type == 'attrName') {
          path[i] = resolveAttrName(path[i].name)
        }
      }
      if (path.length > 1) {
        nestedPaths[path[0]] = true
      }
      pathHeads[path[0]] = true
      return path
    }

Function
  = !ReservedWord head:IdentifierStart tail:IdentifierPart* _ '(' _ args:FunctionArgumentList _ ')' {
      checkMisusedFunction(args)
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
      return {type: 'attrName', name: head + tail.join('')}
    }

ExpressionAttributeValue
  = !ReservedWord head:':' tail:IdentifierPart* {
      return resolveAttrVal(head + tail.join(''))
    }

PathExpression
  = head:GroupedPathExpression tail:(
      _ '[' _ ix:[0-9]+ _ ']' {
        return +(ix.join(''))
      }
    / _ '.' _ prop:Identifier {
        return prop
      }
    )* {
      return (Array.isArray(head) ? head : [head]).concat(tail)
    }

GroupedPathExpression
  = Identifier
  / '(' _ '(' _ path:PathExpression _ ')' _ ')' {
      redundantParensError()
      return path
    }
  / '(' _ path:PathExpression _ ')' {
      return path
    }

Identifier
  = !ReservedWord head:IdentifierStart tail:IdentifierPart* {
      return head + tail.join('')
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
  = BetweenToken
  / InToken
  / AndToken
  / OrToken
  / NotToken

BetweenToken = 'BETWEEN'i !AttributePart
InToken = 'IN'i !AttributePart
AndToken = 'AND'i !AttributePart
OrToken = 'OR'i !AttributePart
NotToken = 'NOT'i !AttributePart

_ 'whitespace'
  = [ \t\r\n]*
