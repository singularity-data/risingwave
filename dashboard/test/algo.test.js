import { Node } from "../lib/algo";
import { dagLayout } from "../lib/streamChartHelper";

describe("Algo", () => {
  it("shoule generate right dag layout", () => {
    // fake data
    let nodes = [];
    for (let i = 0; i < 10; ++i) {
      nodes.push(new Node([], i + 1));
    }
    const n = (i) => nodes[i - 1];
    n(1).nextNodes = [n(2), n(3)];
    n(2).nextNodes = [n(9)];
    n(3).nextNodes = [n(5), n(10)];
    n(4).nextNodes = [n(5)];
    n(5).nextNodes = [n(6), n(7)];
    n(6).nextNodes = [n(9), n(10)];
    n(7).nextNodes = [n(8)];

    let dagPositionMapper = dagLayout(nodes);

    // construct map
    let maxLayer = 0;
    let maxRow = 0;
    for (let node of dagPositionMapper.keys()) {
      let pos = dagPositionMapper.get(node);
      maxLayer = pos[0] > maxLayer ? pos[0] : maxLayer;
      maxRow = pos[1] > maxRow ? pos[1] : maxRow;
    }
    let m = [];
    for (let i = 0; i < maxLayer + 1; ++i) {
      m.push([]);
      for (let r = 0; r < maxRow + 1; ++r) {
        m[i].push([]);
      }
    }
    for (let node of dagPositionMapper.keys()) {
      let pos = dagPositionMapper.get(node);
      m[pos[0]][pos[1]] = node;
    }

    // search
    const _search = (l, r, d) => {// Layer, Row
      if (l > maxLayer || r > maxRow || r < 0) {
        return false;
      }
      if (m[l][r].id !== undefined) {
        return m[l][r].id === d;
      }
      return _search(l + 1, r, d);
    }

    const canReach = (node, nextNode) => {
      let pos = dagPositionMapper.get(node);
      for (let r = 0; r <= maxRow; ++r) {
        if (_search(pos[0] + 1, r, nextNode.id)) {
          return true;
        }
      }
      return false;
    }

    //check all links
    let ok = true;
    for (let node of nodes) {
      for (let nextNode of node.nextNodes) {
        if (!canReach(node, nextNode)) {
          console.error(`Failed to connect node ${node.id} to node ${nextNode.id}`);
          ok = false;
          break;
        }
      }
      if (!ok) {
        break;
      }
    }

    // visualization
    // let s = "";
    // for(let r = maxRow; r >= 0; --r){
    //   for(let l = 0; l <= maxLayer; ++l){
    //     s += `\t${m[l][r].id ? m[l][r].id : " "}`
    //   }
    //   s += "\n"
    // }
    // console.log(s);

    expect(ok).toEqual(true);
  })
})