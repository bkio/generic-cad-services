/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System.Collections.Generic;

namespace CADFileService.Endpoints.Common
{
    public static class SupportedFileFormats
    {
        //Keys should be lowered and should start with dot.
        public static readonly Dictionary<string, string> Formats = new Dictionary<string, string>()
        {
            [".zip"] = "Zip Archive",
            [".rvm"] = "Aveva", [".att"] = "Aveva",/* [".datal"] = "Aveva", [".out"] = "Aveva",*/ //TODO: att,datal,out support?
            [".nwd"] = "Autodesk Navisworks", //TODO: nwd support?
            [".sldasm"] = "Solidworks", [".sldprt"] = "Solidworks",
            [".pxz"] = "PiXYZ",
            [".3ds"] = "Autodesk 3ds Max",
            [".sat"] = "ACIS", [".sab"] = "ACIS",
            [".dwg"] = "Autodesk AutoCAD 3D", [".dxf"] = "AutoCAD 3D",
            [".fbx"] = "Autodesk FBX",
            [".ipt"] = "Autodesk Inventor", [".iam"] = "Autodesk Inventor",
            [".rvt"] = "Autodesk Revit", [".rfa"] = "Autodesk Revit",
            [".model"] = "CATIA", [".session"] = "CATIA",
            [".catpart"] = "CATIA", [".catproduct"] = "CATIA", [".catshape"] = "CATIA", [".cgr"] = "CATIA",
            [".3dxml"] = "CATIA",
            [".asm"] = "PTC - Creo - Pro/Engineer", [".neu"] = "PTC - Creo - Pro/Engineer", [".prt"] = "PTC - Creo - Pro/Engineer", [".xas"] = "PTC - Creo - Pro/Engineer", [".xpr"] = "PTC - Creo - Pro/Engineer",
            [".dae"] = "COLLADA",
            [".csb"] = "CSB Deltagen",
            [".gltf"] = "glTF", [".glb"] = "glTF",
            [".ifc"] = "IFC",
            [".igs"] = "IGES", [".iges"] = "IGES",
            [".jt"] = "Siemens JT",
            [".obj"] = "OBJ",
            [".x_b"] = "Parasolid", [".x_t"] = "Parasolid", [".p_t"] = "Parasolid", [".p_b"] = "Parasolid", [".xmt"] = "Parasolid", [".xmt_txt"] = "Parasolid", [".xmt_bin"] = "Parasolid",
            [".pdf"] = "PDF",
            [".plmxml"] = "Siemens PLM",
            [".e57"] = "Point Cloud", [".pts"] = "Point Cloud", [".ptx"] = "Point Cloud",
            [".prc"] = "Adobe Acrobat 3D",
            [".3dm"] = "Rhino3D",
            [".skp"] = "SketchUp",
            [".asm"] = "Siemens Solid Edge", [".par"] = "Solid Edge", [".pwd"] = "Solid Edge", [".psm"] = "Solid Edge",
            [".stp"] = "STEP", [".step"] = "STEP", [".stpz"] = "STEP", [".stepz"] = "STEP",
            [".stl"] = "StereoLithography",
            [".u3d"] = "U3D",
            [".prt"] = "Siemens Unigraphics-NX",
            [".vda"] = "VDA-FS",
            [".wrl"] = "VRML", [".wrml"] = "VRML"
        };
    }
}