import path from "node:path";
import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import protobuf from "protobufjs";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const exampleRoot = path.resolve(__dirname, "..");
const repoRoot = path.resolve(exampleRoot, "..", "..");
const localProtoRoot = path.join(repoRoot, "protobuf-sources", "src", "main", "proto");
const generatedProtoRoot = path.join(repoRoot, "protobuf-sources", "block-node-protobuf");

const entryFiles = [
  path.join(localProtoRoot, "internal", "unparsed.proto"),
  path.join(generatedProtoRoot, "services", "transaction.proto"),
  path.join(generatedProtoRoot, "services", "transaction_contents.proto"),
];

let rootPromise: Promise<protobuf.Root> | undefined;

function candidatePaths(origin: string, target: string): string[] {
  const baseDir = origin ? path.dirname(origin) : localProtoRoot;
  return [
    path.resolve(baseDir, target),
    path.resolve(localProtoRoot, target),
    path.resolve(generatedProtoRoot, target),
  ];
}

export function getRepoRoot(): string {
  return repoRoot;
}

export async function loadProtoRoot(): Promise<protobuf.Root> {
  if (rootPromise !== undefined) {
    return rootPromise;
  }

  rootPromise = (async () => {
    const root = new protobuf.Root();
    root.resolvePath = (origin, target) => {
      for (const candidate of candidatePaths(origin, target)) {
        if (existsSync(candidate)) {
          return candidate;
        }
      }
      return target;
    };

    await root.load(entryFiles, { keepCase: false });
    root.resolveAll();
    return root;
  })();

  return rootPromise;
}

