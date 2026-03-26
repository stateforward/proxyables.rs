use proxyables::protocol::{
    create_apply_instruction, create_construct_instruction, create_execute_instruction,
    create_get_instruction, create_release_instruction, InstructionKind, ValueKind,
};
use rmpv::Value;

#[test]
fn dsl_constants_match_shared_contract() {
    assert_eq!(ValueKind::Reference as u32, 0x5a1b3c4d);
    assert_eq!(InstructionKind::Execute as u32, 0xa01e3d98);
    assert_eq!(InstructionKind::Release as u32, 0x1a2b3c4d);
}

#[test]
fn dsl_instruction_shapes_are_canonical() {
    assert_eq!(
        create_get_instruction("key").data,
        Value::Array(vec![Value::String("key".into())])
    );
    assert_eq!(
        create_apply_instruction(vec![Value::from(1), Value::from(2)]).data,
        Value::Array(vec![Value::from(1), Value::from(2)])
    );
    assert_eq!(
        create_construct_instruction(vec![Value::from(1), Value::from(2)]).data,
        Value::Array(vec![Value::from(1), Value::from(2)])
    );
    assert_eq!(
        create_release_instruction("ref-1").data,
        Value::Array(vec![Value::String("ref-1".into())])
    );
    assert_eq!(
        create_execute_instruction(vec![create_get_instruction("key")]).kind,
        InstructionKind::Execute as u32
    );
}
