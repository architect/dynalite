exports.types = {
  RequestItems: {
    type: 'Map',
    children: {
      type: 'List',
      children: {
        type: 'Structure',
        children: {
          DeleteRequest: {
            type: 'Structure',
            children: {
              Key: {
                type: 'Map',
                children: {
                  type: 'Structure',
                  children: {
                    S: 'String',
                    B: 'Blob',
                    N: 'String',
                  }
                }
              }
            }
          },
          PutRequest: {
            type: 'Structure',
            children: {
              Item: {
                type: 'Map',
                children: {
                  type: 'Structure',
                  children: {
                    S: 'String',
                    B: 'Blob',
                    N: 'String',
                    BS: {
                      type: 'List',
                      children: 'Blob',
                    },
                    NS: {
                      type: 'List',
                      children: 'String',
                    },
                    SS: {
                      type: 'List',
                      children: 'String',
                    }
                  }
                }
              },
            }
          }
        },
      }
    }
  },
  ReturnConsumedCapacity: 'String',
  ReturnItemCollectionMetrics: 'String',
}

exports.validations = {
  ReturnConsumedCapacity: {
    enum: ['TOTAL', 'NONE']
  },
}

exports.custom = function(data) {
}

