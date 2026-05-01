import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import sharp from "sharp";
import toIco from "to-ico";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const svgPath = path.join(root, "public", "favicon.svg");
const icoPath = path.join(root, "public", "favicon.ico");

const sizes = [16, 32];
const buffers = await Promise.all(sizes.map((s) => sharp(svgPath).resize(s, s).png().toBuffer()));
const ico = await toIco(buffers);
fs.writeFileSync(icoPath, ico);
