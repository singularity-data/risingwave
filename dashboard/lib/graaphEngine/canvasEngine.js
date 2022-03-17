import { fabric } from "fabric";
fabric.Object.prototype.objectCaching = false;
fabric.Object.prototype.statefullCache = false;
fabric.Object.prototype.noScaleCache = true;
fabric.Object.prototype.needsItsOwnCache = () => false;

export class DrawElement {
  /**
   * @param {{svgElement: d3.Selection<any, any, any, any>}} props 
   */
  constructor(props) {
    /**
     * @type {{svgElement: d3.Selection<any, any, any, any>}}
     */
    this.props = props;
    if (props.canvasElement) {
      props.engine.canvas.add(props.canvasElement);
    }
  }

  _attrMap(key, value) {
    return [key, value];
  }

  attr(key, value) {
    let setting = this._attrMap(key, value);
    if (setting && setting.length === 2) {
      this.props.canvasElement && this.props.canvasElement.set(setting[0], setting[1]);
    }
    return this;
  }

  _afterPosition() {
    let ele = this.props.canvasElement;
    ele && this.props.engine.addCanvasElement(ele);
  }


  position(x, y) {
    this.props.canvasElement.set("left", x);
    this.props.canvasElement.set("top", y);
    this._afterPosition();
    return this;
  }

  on(event, callback) {
    return this;
  }

  style(key, value) {
    return this.attr(key, value);
  }

  classed(clazz, flag) {
    this.props.engine.classedElement(clazz, this, flag);
    return this;
  }
}

export class Group extends DrawElement {
  /**
   * @param {{engine: CanvasEngine}} props 
   */
  constructor(props) {
    super(props);

    this.appendFunc = {
      "g": this._appendGroup,
      "circle": this._appendCircle,
      "rect": this._appendRect,
      "text": this._appendText,
      "path": this._appendPath,
      "polygon": this._appendPolygan,
    }

    this.basicSetting = {
      engine: props.engine
    }
  }

  _appendGroup = () => {
    return new Group(this.basicSetting);
  }

  _appendCircle = () => {
    return new Circle({
      ...this.basicSetting, ...{
        canvasElement: new fabric.Circle({ selectable: false })
      }
    });
  }

  _appendRect = () => {
    return new Rectangle({
      ...this.basicSetting, ...{
        canvasElement: new fabric.Rect({ selectable: false })
      }
    });
  }

  _appendText = () => {
    return (content) => new Text({
      ...this.basicSetting, ...{
        canvasElement: new fabric.Text(content, { selectable: false, textAlign: "justify-center" })
      }
    });
  }

  _appendPath = () => {
    return (d) => new Path({
      ...this.basicSetting, ...{
        canvasElement: new fabric.Path(d, { selectable: false })
      }
    });
  }

  _appendPolygan = () => {
    return new Polygan(this.basicSetting);
  }

  append = (type) => {
    return this.appendFunc[type]();
  }
}

export class Rectangle extends DrawElement {
  /**
   * @param {{g: fabric.Group}} props 
   */
  constructor(props) {
    super(props);
    this.props = props;
  }

  init(x, y, width, height) {
    let ele = this.props.canvasElement;
    ele.set("left", x);
    ele.set("top", y);
    ele.set("width", width);
    ele.set("height", height);
    super._afterPosition();
    return this;
  }

  _attrMap(key, value) {
    if (key === "rx") {
      this.props.canvasElement.set("rx", value);
      this.props.canvasElement.set("ry", value);
      return false;
    }
    return [key, value];
  }
}

export class Circle extends DrawElement {
  /**
   * @param {{svgElement: d3.Selection<SVGCircleElement, any, any, any>}} props 
   */
  constructor(props) {
    super(props);
    this.props = props;
    this.radius = 0;
  }

  init(x, y, r) {
    this.props.canvasElement.set("left", x - r);
    this.props.canvasElement.set("top", y - r);
    this.props.canvasElement.set("radius", r);
    super._afterPosition();
    return this;
  }

  _attrMap(key, value) {
    if (key === "r") {
      this.radius = value;
      return ["radius", value];
    }
    if (key === "cx") {
      return ["left", value - this.radius];
    }
    if (key === "cy") {
      return ["top", value - this.radius];
    }
    return [key, value];
  }
}

export class Text extends DrawElement {
  /**
   * @param {{svgElement: d3.Selection<d3.Selection<any, any, any, any>, any, null, undefined>}} props 
   */
  constructor(props) {
    super(props)
    this.props = props; 
  }

  position(x, y){
    let e = this.props.canvasElement;
    e.set("top", y);
    e.set("left", x);
    super._afterPosition();
    return this;
  }

  _attrMap(key, value) {
    if (key === "text-anchor") {
      return ["textAlign", value];
    }
    if (key === "font-size") {
      return ["fontSize", value];
    }
    return [key, value];
  }

  text(content) {
    return this;
  }

  getWidth() {

  }
}

export class Polygan extends DrawElement {
  constructor(props) {
    super(props);
    this.props = props;
  }
}

export class Path extends DrawElement {
  constructor(props) {
    super(props);
    this.props = props;
    this.strokeWidth = 1;
    super._afterPosition();
  }


  _attrMap(key, value) {
    if (key === "fill") {
      return ["fill", value === "none" ? false : value];
    }
    if (key === "stroke-width") {
      this.props.canvasElement.set("top", this.props.canvasElement.get("top") - value / 2)
      return ["strokeWidth", value];
    }
    if (key === "stroke-dasharray") {
      return ["strokeDashArray", value.split(",")];
    }
    if (key === "layer") {
      if (value === "back") {
        this.props.canvasElement.canvas.sendToBack(this.props.canvasElement);
      }
      return false;
    }
    return [key, value];
  }

}

// TODO: Use rbtree
class CordMapper {
  constructor() {
    this.map = new Map();
  }

  rangeQuery(start, end) {
    let rtn = new Set();
    for (let [k, s] of this.map.entries()) {
      if (start <= k && k <= end) {
        s.forEach(v => rtn.add(v));
      }
    }
    return rtn;
  }

  insert(k, v) {
    if (this.map.has(k)) {
      this.map.get(k).add(v);
    } else {
      this.map.set(k, new Set([v]));
    }
  }
}

class GridMapper {
  constructor() {
    this.xMap = new CordMapper();
    this.yMap = new CordMapper();
    this.gs = 100; // grid size
  }

  _getKey(value) {
    return Math.round(value / this.gs);
  }

  addObject(minX, maxX, minY, maxY, ele) {
    for (let i = minX; i <= maxX + this.gs; i += this.gs) {
      this.xMap.insert(this._getKey(i), ele);
    }
    for (let i = minY; i <= maxY + this.gs; i += this.gs) {
      this.yMap.insert(this._getKey(i), ele);
    }
  }

  areaQuery(minX, maxX, minY, maxY) {
    let xs = this.xMap.rangeQuery(this._getKey(minX), this._getKey(maxX));
    let ys = this.yMap.rangeQuery(this._getKey(minY), this._getKey(maxY));
    let rtn = new Set();
    xs.forEach(e => {
      if (ys.has(e)) {
        rtn.add(e);
      }
    })
    return rtn;
  }
}

export class CanvasEngine {
  constructor(canvasId, height, width) {
    let canvas = new fabric.Canvas(canvasId);
    // canvas.selection = false;

    this.height = height;
    this.width = width;
    this.canvas = canvas;
    this.clazzMap = new Map();
    this.topGroup = new Group({ engine: this });
    this.gridMapper = new GridMapper();

    let that = this;
    canvas.on('mouse:wheel', function (opt) {
      var evt = opt.e
      if (evt.ctrlKey === true) {
        var delta = opt.e.deltaY;
        var zoom = canvas.getZoom();
        zoom *= 0.999 ** delta;
        if (zoom > 10) zoom = 10;
        if (zoom < 0.03) zoom = 0.03;
        canvas.zoomToPoint({ x: opt.e.offsetX, y: opt.e.offsetY }, zoom)
        evt.preventDefault();
        evt.stopPropagation();

      } else {
        canvas.setZoom(canvas.getZoom()); // essential for rendering (seems like a bug)
        let vpt = this.viewportTransform;
        vpt[4] -= evt.deltaX;
        vpt[5] -= evt.deltaY;
        this.requestRenderAll();
        evt.preventDefault();
        evt.stopPropagation();

        that.refreshView();
      }
    });

    canvas.on('mouse:down', function (opt) {
      var evt = opt.e;
      this.isDragging = true;
      this.selection = false;
      this.lastPosX = evt.clientX;
      this.lastPosY = evt.clientY;
    });

    canvas.on('mouse:move', function (opt) {
      if (this.isDragging) {
        var e = opt.e;
        var vpt = this.viewportTransform;
        vpt[4] += e.clientX - this.lastPosX;
        vpt[5] += e.clientY - this.lastPosY;
        this.requestRenderAll()
        this.lastPosX = e.clientX;
        this.lastPosY = e.clientY;
      }
    });
    canvas.on('mouse:up', function (opt) {
      this.setViewportTransform(this.viewportTransform);
      this.isDragging = false;
      this.selection = true;
    });
  }

  async refreshView() {
    const padding = 50;
    let vpt = this.canvas.viewportTransform;
    let zoom = this.canvas.getZoom();
    let cameraWidth = this.width
    let cameraHeight = this.height;
    let minX = -vpt[4] - padding;
    let maxX = -vpt[4] + cameraWidth + padding;
    let minY = -vpt[5] - padding;
    let maxY = -vpt[5] + cameraHeight + padding;
    let visibleSet = this.gridMapper.areaQuery(minX / zoom, maxX / zoom, minY / zoom, maxY / zoom);

    this.canvas.getObjects().forEach(e => {
      if (visibleSet.has(e)) {
        e.visible = true;
      } else {
        e.visible = false;
      }
    });
    this.canvas.requestRenderAll();
  }

  addCanvasElement(canvasElement) {
    this.gridMapper.addObject(
      canvasElement.left,
      canvasElement.left + canvasElement.width,
      canvasElement.top,
      canvasElement.top + canvasElement.height,
      canvasElement
    )
  }

  /**
   * 
   * @param {string} clazz 
   * @param {DrawElement} element 
   */
  classedElement(clazz, element, flag) {
    if (!flag) {
      this.clazzMap.has(clazz) && this.clazzMap.get(clazz).delete(element);
    } else {
      if (this.clazzMap.has(clazz)) {
        this.clazzMap.get(clazz).add(element);
      } else {
        this.clazzMap.set(clazz, new Set([element]));
      }
    }
  }

  locateTo(selector) {
    //
    let selectorSet = this.clazzMap.get(selector);
    if (selectorSet) {
      let arr = Array.from(selectorSet);
      if (arr.length > 0) {
        let ele = arr[0];
        let x = ele.props.canvasElement.get("left");
        let y = ele.props.canvasElement.get("top");
        let scale = 0.6;
        this.canvas.setZoom(scale);
        let vpt = this.canvas.viewportTransform;
        vpt[4] = (-x + this.width * 0.5) * scale;
        vpt[5] = (-y + this.height * 0.5) * scale;
        this.canvas.requestRenderAll();
        this.refreshView();
      }
    }
  }

  resetCamera() {
    let zoom = this.canvas.getZoom();
    zoom *= 0.999;
    this.canvas.setZoom(zoom);
    let vpt = this.canvas.viewportTransform;
    vpt[4] = 0;
    vpt[5] = 0;
    this.canvas.requestRenderAll();
    this.refreshView();
  }

  cleanGraph() {
    console.log("clean called");
    this.canvas.dispose();
  }

  resize(width, height) {
    this.width = width;
    this.height = height;
    this.canvas.setDimensions({ width: this.width, height: this.height });
  }
}