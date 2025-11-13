const result = await Bun.build({ packages: "bundle", entrypoints: ["./index.ts"], compile: true, outdir: "bin/" });
console.log(`wrote db.out bytes=${result.outputs[0]?.size}`)

