exports.types = {
  ReturnConsumedCapacity: {
    type: 'String',
    enum: ['TOTAL', 'NONE']
  },
  ReturnItemCollectionMetrics: {
    type: 'String',
    enum: ['SIZE', 'NONE']
  },
  RequestItems: {
    type: 'Map',
    requiredMap: true,
    tableMap: true,
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
                notNull: true,
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
              }
            }
          },
          PutRequest: {
            type: 'Structure',
            children: {
              Item: {
                type: 'Map',
                notNull: true,
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
}

exports.custom = function(data) {
}

