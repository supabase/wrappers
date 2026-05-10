// Shopify Admin GraphQL API mock server for FDW integration tests.
// Dispatches on the root field/mutation name extracted from the query.

function extractOperation(query: string): { kind: "query" | "mutation"; name: string } {
  const trimmed = query.trim();
  if (trimmed.startsWith("mutation")) {
    const m = trimmed.match(/mutation\s+(\w+)/);
    return { kind: "mutation", name: m ? m[1] : "unknown" };
  }
  // Match `query { field` or `query($var: Type) { field`
  const m = trimmed.match(/^query\s*(?:\([^)]*\))?\s*\{\s*([a-zA-Z]+)/);
  return { kind: "query", name: m ? m[1] : "unknown" };
}

async function loadFixture(path: string): Promise<object | null> {
  const f = Bun.file(path);
  if (await f.exists()) return f.json();
  return null;
}

const server = Bun.serve({
  port: 3000,
  async fetch(req) {
    const url = new URL(req.url);

    if (url.pathname === "/health") {
      return new Response("ok", { status: 200 });
    }

    if (!url.pathname.match(/^\/admin\/api\/[\w-]+\/graphql\.json$/)) {
      return new Response("not found", { status: 404 });
    }

    let body: { query?: string; variables?: Record<string, unknown> } = {};
    try {
      body = await req.json();
    } catch {
      return Response.json({ errors: [{ message: "invalid JSON body" }] }, { status: 400 });
    }

    const query = body.query;
    if (!query) {
      return Response.json({ errors: [{ message: "missing query" }] }, { status: 400 });
    }

    const variables = body.variables ?? {};
    const op = extractOperation(query);

    if (op.kind === "mutation") {
      const fixture = await loadFixture(`./fixtures/mutations/${op.name}.json`);
      if (fixture) return Response.json(fixture);
      return Response.json({ data: { [op.name]: { userErrors: [] } } });
    }

    // Nested query: variables contain "parentId" — dispatch to fixtures/nested/<parent>.json
    if (variables.parentId !== undefined) {
      const fixture = await loadFixture(`./fixtures/nested/${op.name}.json`);
      if (fixture) return Response.json(fixture);
    }

    // Singular query: variables contain "id"
    if (variables.id !== undefined) {
      const fixture = await loadFixture(`./fixtures/singular/${op.name}.json`);
      if (fixture) return Response.json(fixture);
    }

    // Pagination page 2: cursor is non-null
    if (variables.cursor !== undefined && variables.cursor !== null) {
      const fixture = await loadFixture(`./fixtures/${op.name}_page2.json`);
      if (fixture) return Response.json(fixture);
    }

    // Default list fixture
    const fixture = await loadFixture(`./fixtures/${op.name}.json`);
    if (fixture) return Response.json(fixture);

    return Response.json({
      data: {
        [op.name]: { nodes: [], pageInfo: { hasNextPage: false, endCursor: null } },
      },
    });
  },
});

console.log(`Shopify mock server listening on :${server.port}`);
