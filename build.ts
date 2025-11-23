import { $ } from "bun";

const version = await $`git describe --tags --always`.text();
const buildTime = new Date().toISOString();
const gitCommit = await $`git rev-parse HEAD`.text();

const result = await Bun.build({
    packages: "bundle",
    entrypoints: ["./index.ts"],
    compile: true,
    minify: true,
    bytecode: true,
    outdir: "bin",
    env: "inline",
    define: {
        BUILD_TIME: JSON.stringify(buildTime),
        VERSION: JSON.stringify(version),
        COMMIT: JSON.stringify(gitCommit),
        PRODUCTION: JSON.stringify(true)
    }
});
console.log(`wrote ./bin bytes=${result.outputs[0]?.size}`)

