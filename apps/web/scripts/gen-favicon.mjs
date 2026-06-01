import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import pngToIco from "png-to-ico";
import sharp from "sharp";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const svgPath = path.join(root, "public", "favicon.svg");
const icoPath = path.join(root, "public", "favicon.ico");

const sizes = [16, 32];
const buffers = await Promise.all(sizes.map((s) => sharp(svgPath).resize(s, s).png().toBuffer()));
const ico = await pngToIco(buffers);
fs.writeFileSync(icoPath, ico);
