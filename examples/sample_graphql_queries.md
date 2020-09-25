# Example queries

## Display 25 random papers with some metadata
```graphql
{
    Get {
        Things {
            Paper (limit: 25) {
                title
                abstract
                year
                InJournal {
                    ... on Journal {
                        name
                    }
                }
                HasAuthors {
                    ... on Author {
                        name
                    }
                }
                HasCategories {
                    ... on Category {
                        name
                    }
                }
            }
        }
    }
}
```

## Get categories
```graphql
{
    Get {
        Things {
            Category (limit: 25) {
                name
                InArchive {
                    ... on Archive {
                        name
                    }
                }
            }
        }
    }
}
```


## Get how many papers per category, to create the items in the Category filter
### THIS CANNOT BE DONE YET (feel free to create an issue or let me know so I create an issue for Weaviate)
```graphql
{
  Aggregate {
    Things {
      Paper (groupBy:["HasCategories", "Category", "name"]) {
        meta {
          count
        }
        groupedBy {
          value
        }
      }
    }
  }
}
```

### workaround per category (so first get the category names)
```graphql
{
  Aggregate {
    Things {
      Paper (where: {
        path: ["HasCategories", "Category", "name"]
        operator:Equal
        valueString: "Artificial Intelligence"
      }){
        meta {
          count
        }
      }
    }
  }
}
```

## If a category is selected, the papers can be found like this:
```graphql
{
    Get {
        Things {
            Paper (
                limit: 25,
                where: {
                    path: ["HasCategories", "Category", "name"],
                    operator: Equal,
                    valueString: "Artificial Intelligence"
                }) {
                title
                abstract
                year
                InJournal {
                    ... on Journal {
                        name
                    }
                }
                HasAuthors {
                    ... on Author {
                        name
                    }
                }
            }
        }
    }
}
```

## With search bar:
```graphql
{
    Get {
        Things {
            Paper (
                limit: 25,
                where: {
                    path: ["HasCategories", "Category", "name"],
                    operator: Equal,
                    valueString: "Artificial Intelligence"
                },
                explore: {
                    concepts: ["neural network"], 
                    certainty: 0.5
                }) {
                title
                abstract
                year
                InJournal {
                    ... on Journal {
                        name
                    }
                }
                HasAuthors {
                    ... on Author {
                        name
                    }
                }
            }
        }
    }
}
```