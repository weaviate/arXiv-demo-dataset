# Example queries

## Display 25 random papers with some metadata
```graphql
{
  Get {
    Paper(limit: 25) {
      title
      abstract
      year
      inJournal {
        ... on Journal {
          name
        }
      }
      hasAuthors {
        ... on Author {
          name
        }
      }
      hasCategories {
        ... on Category {
          name
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
    Category(limit: 25) {
      name
      inArchive {
        ... on Archive {
          name
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
```

### workaround per category (so first get the category names)
```graphql
{
  Aggregate {
    Paper(
      where: {
        path: ["hasCategories", "Category", "name"],
        operator: Equal,
        valueString: "Computational Geometry"}
    ) {
      meta {
        count
      }
    }
  }
}
```

## If a category is selected, the papers can be found like this:
```graphql
{
  Get {
    Paper(
      limit: 25
      where: {
        path: ["hasCategories", "Category", "name"],
        operator: Equal,
        valueString: "Computational Geometry"}
    ) {
      title
      abstract
      year
      inJournal {
        ... on Journal {
          name
        }
      }
      hasAuthors {
        ... on Author {
          name
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
    Paper (
      limit:25,
      where : {
        path: ["hasCategories", "Category", "name"]
        operator:Equal
        valueString:"Computational Geometry"
      }
      nearText: {
        concepts: ["robotics"]
        certainty:0.5
      }
    ) {
      title
      abstract
      year
      inJournal {
        ... on Journal {
          name
        }
      }
      hasAuthors {
        ... on Author {
          name
        }
      }
    }
  }
}
```
