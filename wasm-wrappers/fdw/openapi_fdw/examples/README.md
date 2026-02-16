# Examples

Each example shows how to configure the FDW against a real API, with complete server options, table definitions, and sample queries.

## No Auth Required

| Example | API | Features |
| --- | --- | --- |
| [pokeapi](pokeapi/) | [PokéAPI](https://pokeapi.co/) | Offset-based pagination, path params, auto-detected `results` wrapper |
| [carapi](carapi/) | [CarAPI](https://carapi.app/) | Page-based pagination, query pushdown, auto-detected `data` wrapper |
| [nws](nws/) | [National Weather Service](https://www.weather.gov/documentation/services-web-api) | GeoJSON responses, nested path extraction, custom User-Agent |

## Auth Required

| Example | API | Auth | Features |
| --- | --- | --- | --- |
| [github](github/) | [GitHub REST API](https://docs.github.com/en/rest) | Bearer token | Path params, custom headers, `items` wrapper, search pushdown |
| [threads](threads/) | [Meta Threads API](https://developers.facebook.com/docs/threads) | OAuth token (query param) | Cursor-based pagination, path params, query pushdown |
