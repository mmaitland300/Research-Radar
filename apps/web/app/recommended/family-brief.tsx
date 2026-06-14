import type { Family } from "./types";
import { FAMILY_LABEL, FAMILY_NOTES } from "./ranking-copy";

export function FamilyBrief({ family }: { family: Family }) {
  return (
    <aside className="family-brief">
      <div className="family-brief-diagram" aria-hidden="true">
        <span className="family-ring family-ring-a" />
        <span className="family-ring family-ring-b" />
        <span className="family-ring family-ring-c" />
        <span className={`family-sweep family-sweep-${family}`} />
        <span className={`family-node family-node-${family} family-node-1`} />
        <span className={`family-node family-node-${family} family-node-2`} />
        <span className={`family-node family-node-${family} family-node-3`} />
      </div>
      <div className="family-brief-copy">
        <p className={`eyebrow family-${family}`}>Signal lens</p>
        <h2>
          {family === "bridge"
            ? "Bridge preview reading guide"
            : `${FAMILY_LABEL[family]} reading guide`}
        </h2>
        <ul className="measure-list">
          {FAMILY_NOTES[family].map((note) => (
            <li key={note}>{note}</li>
          ))}
        </ul>
      </div>
    </aside>
  );
}
